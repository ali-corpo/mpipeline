from __future__ import annotations
import multiprocessing as mp
from dataclasses import dataclass
from typing import Generic, TypeVar, Iterator, List, Any, Tuple, Iterable
import threading
from multiprocessing.pool import ThreadPool
import asyncio
from time import perf_counter
import warnings
from .stage import Stage
from .worker_exception import WorkerException
from .pipeline_tqdm import PipelineTQDM
# Default to spawn context
DEFAULT_MP_CONTEXT = 'spawn'

# Use spawn context globally
mp_ctx = mp.get_context(DEFAULT_MP_CONTEXT)

# Thread-local storage for worker instances
_local = threading.local()

T = TypeVar('T')
Q = TypeVar('Q')
Z = TypeVar('Z')


@dataclass
class StageConfig:
    """Configuration for a pipeline stage."""
    worker_count: int = 1
    queue_size: int = 100
    use_threads: bool = False
    # Default to spawn, but allow 'fork' or other contexts
    mp_context: str = DEFAULT_MP_CONTEXT


def _cleanup_worker(_: Any = None) -> None:
    """Clean up worker resources when pool shuts down."""
    if hasattr(_local, 'worker'):
        _local.worker.__dispose__()


def _init_worker(stage: Stage[T, Q], stage_idx: int) -> None:
    """Initialize worker in the pool process/thread."""
    try:
        _local.worker = stage.worker_class(*stage.worker_args, **stage.worker_kwargs)
        _local.stage_idx = stage_idx  # Store stage index for error reporting
        _local.worker.__local_init__()
        import atexit
        atexit.register(_cleanup_worker)
    except BaseException as e:
        raise WorkerException(e, stage.worker_class.__name__, None)


def _process_item(args: Tuple[int, T]) -> Tuple[int, Any, float] | WorkerException:
    """Process a single item using the thread-local worker."""
    seq_num, inp = args
    try:
        start_time = perf_counter()
        result = _local.worker.__process__(inp)
        process_time = perf_counter() - start_time
        return seq_num, result, process_time
    except BaseException as e:
        return WorkerException(e, _local.worker.__class__.__name__, inp)


class Pipeline(Generic[T, Q]):
    """A pipeline that processes data through multiple stages."""

    def __init__(self, stage: Stage[T, Q]):
        """Initialize pipeline with first worker."""
        self.stages: List[Stage[Any, Any]] = [stage]
        self._contexts = {}
        self._pools = []
        self._running = False
        self._progress = None

    def _stop_pools(self) -> None:
        """Stop all worker pools."""
        for pool in self._pools:
            if pool:
                try:
                    # Ensure workers are disposed before termination
                    if hasattr(pool, '_pool'):
                        try:
                            pool.map(_cleanup_worker, [None] * pool._processes)
                        except (KeyboardInterrupt, Exception) as e:
                            # During interrupt, just terminate the pool
                            pass
                    pool.terminate()  # Terminate worker processes
                    pool.close()  # Stop accepting new tasks
                    # Wait for processes to exit with short timeout
                    pool.join()
                except BaseException as e:
                    warnings.warn(f"Error during pool cleanup: {e}")
                finally:
                    # Ensure pool is terminated even if join fails
                    if hasattr(pool, 'terminate'):
                        try:
                            pool.terminate()
                        except:
                            pass
        self._pools = []

    def then(self, stage: Stage[Q, Z]) -> 'Pipeline[T, Z]':
        """Add a stage to the pipeline."""
        self.stages.append(stage)
        return self

    def _get_context(self, stage_idx: int, config: StageConfig) -> mp.context.BaseContext:
        """Get or create multiprocessing context for a stage."""
        if stage_idx not in self._contexts:
            self._contexts[stage_idx] = mp.get_context(config.mp_context)
        return self._contexts[stage_idx]

    def _init_pools(self) -> None:
        """Initialize worker pools for each stage."""
        self._pools = []
        try:
            for idx, stage in enumerate(self.stages):
                if stage.use_threads:
                    pool = ThreadPool(
                        processes=stage.worker_count,
                        initializer=_init_worker,
                        initargs=(stage, idx)
                    )
                else:
                    # Get context for this stage
                    ctx = self._get_context(idx, stage)
                    pool = ctx.Pool(
                        processes=stage.worker_count,
                        initializer=_init_worker,
                        initargs=(stage, idx)
                    )
                self._pools.append(pool)
        except Exception as e:
            self._stop_pools()  # Clean up any created pools
            raise e

    def _process_stage(self, stage_idx: int, iterator, is_final: bool = False) -> Iterator[Any]:
        """Process items through a stage and update progress.

        Args:
            stage_idx: Index of the current stage
            iterator: Iterator over stage items
            is_final: If True, yields only the result instead of (seq_num, data)
        """
        for item in iterator:
            if isinstance(item, WorkerException):
                # Re-raise worker exceptions
                self._running = False
                print("reraising exception")
                item.re_raise()
            if isinstance(item, BaseException):
                raise item
            if not self._running:
                break
            seq_num, data, proc_time = item
            self._progress.update_stage_progress(stage_idx, proc_time)

            if is_final:
                yield data
            else:
                yield seq_num, data

    def run(self, inputs: Iterable[T], ordered_result: bool = True,
            show_progress: bool = False, show_stage_progress: bool = False) -> Iterator[Any]:
        """Run the pipeline on the inputs."""
        if not self.stages:
            raise ValueError("Pipeline has no stages")

        self._running = True
        total = len(inputs) if hasattr(inputs, '__len__') else None
        self._progress = PipelineTQDM(self.stages, show_progress, show_stage_progress, total=total)

        try:
            self._init_pools()
            current_data = enumerate(inputs)

            # Initialize all stages first
            for stage_idx, (stage, pool) in enumerate(zip(self.stages, self._pools)):
                if not self._running:
                    break

                # Process current stage
                results_iter = pool.imap(_process_item, current_data) if ordered_result else \
                    pool.imap_unordered(_process_item, current_data)

                if stage_idx == len(self.stages) - 1:
                    # Last stage - yield results directly
                    yield from self._process_stage(stage_idx, results_iter, is_final=True)
                else:
                    # Intermediate stage - prepare data for next stage
                    current_data = self._process_stage(stage_idx, results_iter)

        except Exception:
            self._running = False

            raise
        finally:
            self._running = False
            if self._progress:
                self._progress.cleanup()
            self._stop_pools()
