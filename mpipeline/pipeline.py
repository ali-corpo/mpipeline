from __future__ import annotations

import asyncio
import multiprocessing as mp
import threading
import warnings
from dataclasses import dataclass
from multiprocessing import Manager
from multiprocessing.pool import ThreadPool
from time import perf_counter
from typing import Any, Generic, Iterable, Iterator, List, Literal, Tuple, TypeVar

from .pipeline_tqdm import PipelineTQDM
from .stage import Stage
from .worker_exception import WorkerException
from .worker import Worker
import atexit
import sys
# Thread-local storage for worker instances
_local = threading.local()
T = TypeVar('T')
Q = TypeVar('Q')
Z = TypeVar('Z')

ProgressType = Literal['total', 'stage', None]

manager=Manager()
force_exit = manager.Value('bool',False)

FORCE_EXIT_EXCEPTION=Exception("Force exit signal received")

def _cleanup_worker(_: Any = None) -> None:
    """Clean up worker resources when pool shuts down."""
    # print("cleaning up ", threading.current_thread().name)
    if hasattr(_local, 'worker'):
        _local.worker.__dispose__()


def _init_worker(stage: Stage[T, Q], stage_idx: int) -> None:
    """Initialize worker in the pool process/thread."""
    try:
        _local.worker = stage.worker_class(*stage.worker_args, **stage.worker_kwargs)
        _local.stage_idx = stage_idx  # Store stage index for error reporting
        _local.worker.__local_init__()
        atexit.register(_local.worker.__dispose__)
    except BaseException as e:
        raise WorkerException(e, stage.worker_class.__name__, None)


def _process_item(args: Tuple[int, T]) -> Tuple[int, Any, float] | WorkerException:
    """Process a single item using the thread-local worker."""
    seq_num, inp = args
    start_time = perf_counter()
    try:
        if force_exit.value:
            raise FORCE_EXIT_EXCEPTION
        result = _local.worker.__process__(inp)
        process_time = perf_counter() - start_time
        return seq_num, result, process_time
    except BaseException as e:
        force_exit.value=True
        _cleanup_worker()
        if isinstance(e, KeyboardInterrupt):
            raise FORCE_EXIT_EXCEPTION
        process_time = perf_counter() - start_time
        return seq_num,WorkerException(e, _local.worker.__class__.__name__, inp),process_time


class Pipeline(Generic[T, Q]):
    """A pipeline that processes data through multiple stages."""

    def __init__(self, stage: Stage[T, Q]):
        """Initialize pipeline with first worker."""
        self.stages: List[Stage[Any, Any]] = [stage]
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
                    pool.map(_cleanup_worker,range(pool._processes),)
                    pool.close()  
                    pool.join()  
                except BaseException as e:
                    warnings.warn(f"Error during pool stop: {e}")
        

    def then(self, stage: Stage[Q, Z]) -> 'Pipeline[T, Z]':
        """Add a stage to the pipeline."""
        self.stages.append(stage)
        return self

    def __or__(self, stage: Stage[Q, Z]) -> 'Pipeline[T, Z]':
        """Add a stage to the pipeline using the | operator.
        
        Example:
            pipeline = Pipeline(stage1) | stage2 | stage3
        """
        return self.then(stage)

    def _get_context(self, stage_idx: int, stage: Stage) -> mp.context.BaseContext:
        if stage_idx not in self._contexts:
            self._contexts[stage_idx] = mp.get_context(stage.multiprocess_mode)
        return self._contexts[stage_idx]

    def _init_pools(self) -> None:
        self._pools = []
        try:
            for idx, stage in enumerate(self.stages):
                if stage.mode == 'thread':
                    pool = ThreadPool(
                        processes=stage.worker_count,
                        initializer=_init_worker,
                        initargs=(stage, idx)
                    )
                else:
                    ctx = self._get_context(idx, stage)
                    pool = ctx.Pool(
                        processes=stage.worker_count,
                        initializer=_init_worker,
                        initargs=(stage, idx)
                    )
                self._pools.append(pool)
        except Exception as e:
            raise e

    def _process_stage(self, stage_idx: int, iterator) -> Iterator[Any]:
        for item in iterator:
            if force_exit.value:
                continue
            seq_num, data, proc_time = item
            if self._progress:
                self._progress.update_stage_progress(stage_idx, proc_time)

            yield seq_num, data

    def run(self, inputs: Iterable[T], ordered_result: bool = True,
            progress: ProgressType = None) -> Iterator[Any]:
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
        force_exit.value=False
        total = len(inputs) if hasattr(inputs, '__len__') else None
        
        self._progress = PipelineTQDM(self.stages, progress, total=total)

        try:
            self._init_pools()
            current_data = enumerate(inputs)
            exception=None
            for stage_idx, (stage, pool) in enumerate(zip(self.stages, self._pools)):
                results_iter = pool.imap(_process_item, current_data) if ordered_result else \
                    pool.imap_unordered(_process_item, current_data)

                if stage_idx == len(self.stages) - 1:
                    for seg_idx,res in self._process_stage(stage_idx, results_iter):
                        if exception is not None:
                            continue
                        if isinstance(res,BaseException):
                            exception=res
                        else:    
                            yield res
                else:
                    current_data = self._process_stage(stage_idx, results_iter)
            if exception is not None:            
                raise exception
        except BaseException as e:            
            force_exit.value=True
            if isinstance(e,WorkerException):
                e.re_raise()
            raise
        finally:
            if self._progress:
                self._progress.cleanup()
            self._stop_pools()