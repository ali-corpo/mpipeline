from __future__ import annotations

import atexit
import multiprocessing as mp
import threading
import warnings
from collections.abc import Iterable
from collections.abc import Iterator
from multiprocessing.pool import ThreadPool
from time import perf_counter
from typing import Any
from typing import Generic
from typing import Literal
from typing import TypeVar

from UltraDict import UltraDict

from .pipeline_tqdm import PipelineTQDM
from .stage import Stage
from .worker_exception import WorkerException
from mpipeline.worker import Worker
# Thread-local storage for worker instances
_local = threading.local()
T = TypeVar('T')
Q = TypeVar('Q')
Z = TypeVar('Z')

ProgressType = Literal['total', 'stage', None]


class ForceExitException(BaseException):
    def __init__(self, *args, **kwargs):
        super().__init__("Force exit signal received")


def _cleanup_worker(_: Any = None, worker=None) -> None:
    """Clean up worker resources when pool shuts down."""
    # print("cleaning up ", threading.current_thread().name)
    if hasattr(_local, 'worker'):
        worker = worker or _local.worker

    if worker:
        worker.__dispose__()


def _init_worker(stage: Stage[T, Q], stage_idx: int, shared_data_name: str) -> Worker:
    """Initialize worker in the pool process/thread."""
    try:
        _local.worker = stage.worker_class(*stage.worker_args, **stage.worker_kwargs)
        _local.worker.__local_init__()
        if not hasattr(_local, 'shared_data'):
            _local.shared_data = UltraDict(name=shared_data_name)
        atexit.register(_local.worker.__dispose__)
        return _local.worker
    except BaseException as e:
        raise WorkerException(e, stage.worker_class.__name__, None, None)


def _process_item(args: tuple[int, T | Exception], worker=None) -> tuple[int, Any | BaseException, float]:
    """Process a single item using the thread-local worker."""
    shared_data = _local.shared_data
    seq_num, inp = args
    start_time = perf_counter()
    worker = worker or _local.worker
    try:
        if isinstance(inp, BaseException):
            raise inp
        if shared_data['_force_exit']:
            raise ForceExitException()
        result = worker.__process__(inp, shared_data)
        process_time = perf_counter() - start_time
        return seq_num, result, process_time
    except BaseException as e:
        shared_data['_force_exit'] = True
        _cleanup_worker()
        # if isinstance(e, KeyboardInterrupt):
        #     raise FORCE_EXIT_EXCEPTION
        process_time = perf_counter() - start_time
        if isinstance(e, WorkerException) or isinstance(e, ForceExitException):
            return seq_num, e, process_time
        return seq_num, WorkerException(e, worker.__class__.__name__, inp, shared_data), process_time


class Pipeline(Generic[T, Q]):
    """A pipeline that processes data through multiple stages."""

    def __init__(self, stage: Stage[T, Q]):
        """Initialize pipeline with first worker."""
        self.stages: list[Stage[Any, Any]] = [stage]
        self._contexts = {}
        self._pools = []
        self._progress = None

    def _terminate_pools(self):
        for pool in self._pools:
            if pool and hasattr(pool, '_pool'):
                pool.close()
                pool.terminate()
                pool.join()
        self._pools = []

    def _stop_pools(self) -> None:
        """Stop all worker pools."""
        for pool in self._pools:
            if pool:
                try:
                    pool.map(_cleanup_worker, range(pool._processes),)
                    pool.close()
                    pool.join()
                except BaseException as e:
                    warnings.warn(f"Error during pool stop: {e}")

    def then(self, stage: Stage[Q, Z]) -> Pipeline[T, Z]:
        """Add a stage to the pipeline."""
        self.stages.append(stage)
        return self

    def __or__(self, stage: Stage[Q, Z]) -> Pipeline[T, Z]:
        """Add a stage to the pipeline using the | operator.

        Example:
            pipeline = Pipeline(stage1) | stage2 | stage3
        """
        return self.then(stage)

    def _get_context(self, stage_idx: int, stage: Stage) -> mp.context.BaseContext:
        if stage_idx not in self._contexts:
            self._contexts[stage_idx] = mp.get_context(stage.multiprocess_mode)
        return self._contexts[stage_idx]

    def _init_pools(self, shared_data: UltraDict) -> None:
        self._pools = []
        try:
            for idx, stage in enumerate(self.stages):
                if stage.mode == 'thread':
                    pool = ThreadPool(
                        processes=stage.worker_count,
                        initializer=_init_worker,
                        initargs=(stage, idx, shared_data.name)
                    )
                else:
                    ctx = self._get_context(idx, stage)
                    pool = ctx.Pool(
                        processes=stage.worker_count,
                        initializer=_init_worker,
                        initargs=(stage, idx, shared_data.name)
                    )
                self._pools.append(pool)
        except Exception as e:
            raise e

    def _process_stage(self, shared_data: dict, stage_idx: int, iterator) -> Iterator[Any]:
        for item in iterator:
            # if shared_data['_force_exit']:
            #     continue
            seq_num, data, proc_time = item
            if self._progress:
                self._progress.update_stage_progress(stage_idx, proc_time)
                if shared_data['_force_exit']:
                    self._progress.set_error()
            yield seq_num, data

    def no_thread_run(self, inputs: Iterable[T], shared_data: UltraDict | None = None, ordered_result: bool = True, progress: ProgressType = None) -> Iterator[Q]:
        if hasattr(_local, 'shared_data'):
            del _local.shared_data
        shared_data_dict = shared_data or UltraDict()
        shared_data_dict['_force_exit'] = False

        total = len(inputs) if hasattr(inputs, '__len__') else None
        workers = [
            _init_worker(stage, stage_idx, shared_data_dict.name)
            for stage_idx, stage in enumerate(self.stages)
        ]
        self._progress = PipelineTQDM(self.stages, progress, total=total)
        try:
            for inp in enumerate(inputs):
                data = None
                for stage_idx, worker in enumerate(workers):
                    seq_num, data, proc_time = _process_item(inp, worker)
                    self._progress.update_stage_progress(stage_idx, proc_time)

                    inp = seq_num, data
                    if isinstance(data, BaseException):
                        raise data
                yield data  # type: ignore
            # if exception is not None:
            #     raise exception
        except BaseException as e:
            if isinstance(e, WorkerException):
                e.re_raise()
            raise e
        finally:
            if self._progress:
                self._progress.cleanup()
            for worker in workers:
                _cleanup_worker(worker=worker)

    def run(self, inputs: Iterable[T], shared_data: UltraDict | None = None, ordered_result: bool = True, progress: ProgressType = None, no_thread: bool = False) -> Iterator[Q]:
        """Run the pipeline on the inputs.

        Args:
            inputs: Input data to process
            ordered_result: If True, maintain input order in output
            progress: Progress tracking mode:
                     - 'total': Show overall progress
                     - 'stage': Show per-stage progress
                     - None: No progress tracking
        """
        if not self.stages:
            raise ValueError("Pipeline has no stages")
        if not no_thread:
            yield from self.no_thread_run(inputs, shared_data, ordered_result, progress)
            return

        shared_data_dict = shared_data if shared_data is not None else UltraDict(shared_lock=True, recurse=True)

        shared_data_dict['_force_exit'] = False
        total = len(inputs) if hasattr(inputs, '__len__') else None

        self._progress = PipelineTQDM(self.stages, progress, total=total)

        try:
            self._init_pools(shared_data_dict)
            current_data = enumerate(inputs)
            for stage_idx, (stage, pool) in enumerate(zip(self.stages, self._pools)):

                results_iter = pool.imap(_process_item, current_data) if ordered_result else \
                    pool.imap_unordered(_process_item, current_data)

                if stage_idx == len(self.stages) - 1:
                    for seg_idx, res in self._process_stage(shared_data_dict, stage_idx, results_iter):
                        # if exception is not None:
                        #     continue
                        if isinstance(res, ForceExitException):
                            continue
                        if isinstance(res, BaseException):
                            # exception = res
                            raise res
                        else:
                            yield res
                else:
                    current_data = self._process_stage(shared_data_dict, stage_idx, results_iter)
            # if exception is not None:
            #     raise exception
        except BaseException as e:
            shared_data_dict['_force_exit'] = True
            if isinstance(e, WorkerException):
                e.re_raise()
            raise
        finally:
            if self._progress:
                self._progress.cleanup()
            self._stop_pools()
