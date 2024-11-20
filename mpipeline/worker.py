from __future__ import annotations
import abc
import asyncio
import threading
from typing import Any, Callable, Generic, Optional, TypeVar
import time
T = TypeVar('T')
Q = TypeVar('Q')


class Worker(abc.ABC, Generic[T, Q]):
    """Abstract base class for pipeline workers."""

    def __init__(self, name: str = ""):
        """Initialize the worker. Called once per thread/process before processing starts."""
        self.name = name or self.__class__.__name__

    def __local_init__(self):
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        if any(asyncio.iscoroutinefunction(func) for func in [self.doTask, self.doDispose]):
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        self.__is_disposed = False 

    @abc.abstractmethod
    def doTask(self, inp: T) -> Q:
        """Process a single input and return the result."""
        pass

    def doDispose(self) -> None:
        """Cleanup resources. Called once after processing ends."""
        pass

    def __dispose__(self):
        """Clean up worker resources."""
        if self.__is_disposed:return
        print("cleanup worker", self,threading.current_thread().name)
        self.__is_disposed = True
        if self.__class__.doDispose!=Worker.doDispose:
            self.__exec__(self.doDispose)
            time.sleep(0.3)
        if self._loop:
            self._loop.close()

    def __str__(self) -> str:
        return self.name

    def __exec__(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function, handling both sync and async cases."""
        result = func(*args, **kwargs)
        if asyncio.iscoroutine(result) and self._loop:
            return self._loop.run_until_complete(result)
        return result

    def __process__(self, inp: T) -> Q:
        """Process a single input with proper async handling."""
        if self.__is_disposed:
            raise RuntimeError("Worker is disposed")
        return self.__exec__(self.doTask, inp)
