from __future__ import annotations

import abc
import asyncio
import time
from collections.abc import Callable
from typing import Any
from typing import Generic
from typing import TypeVar

from mpipeline.thread_safe_dict import ThreadSafeDict
T = TypeVar('T')
Q = TypeVar('Q')


class Worker(abc.ABC, Generic[T, Q]):
    """Abstract base class for pipeline workers."""

    def _init(self):
        self._loop: asyncio.AbstractEventLoop | None = None
        if any(asyncio.iscoroutinefunction(func) for func in [self.doTask, self.doDispose]):
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        self.__is_disposed = False

    @abc.abstractmethod
    def doTask(self, inp: T, *, thread_mode_shared_data: ThreadSafeDict, **kwargs) -> Q:
        """Process a single input and return the result."""

    def doDispose(self) -> None:
        """Cleanup resources. Called once after processing ends."""

    def _dispose(self):
        """Clean up worker resources."""
        if self.__is_disposed:
            return
        # print("cleanup worker", self,threading.current_thread().name)
        self.__is_disposed = True
        if self.__class__.doDispose != Worker.doDispose:
            self.doDispose()
            time.sleep(0.3)
        if self._loop:
            self._loop.close()

    def __str__(self) -> str:
        return self.__class__.__name__

    def _exec(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function, handling both sync and async cases."""
        result = func(*args, **kwargs)
        if asyncio.iscoroutine(result) and self._loop:
            return self._loop.run_until_complete(result)
        return result

    def _process(self, inp: T, shared_data: ThreadSafeDict | None, **kwargs) -> Q:
        """Process a single input with proper async handling."""
        if self.__is_disposed:
            raise RuntimeError("Worker is disposed")
        if shared_data is None:
            return self.doTask(inp=inp, **kwargs)
        return self.doTask(inp, thread_mode_shared_data=shared_data, **kwargs)
        # return self._exec(self.doTask, inp, shared_data, **kwargs)
