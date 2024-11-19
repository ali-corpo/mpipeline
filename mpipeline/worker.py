from __future__ import annotations
import abc
import asyncio
from typing import Any, Callable, Generic, Optional, TypeVar

T = TypeVar('T')
Q = TypeVar('Q')


class Worker(abc.ABC, Generic[T, Q]):
    """Abstract base class for pipeline workers."""

    def __init__(self, name: str = ""):
        self.name = name or self.__class__.__name__

    def __local_init__(self):
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        if any(asyncio.iscoroutinefunction(func) for func in [self.doInit, self.doTask, self.doDispose]):
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        self.__exec__(self.doInit)

    @abc.abstractmethod
    def doTask(self, inp: T) -> Q:
        """Process a single input and return the result."""
        pass

    def doInit(self) -> None:
        """Initialize the worker. Called once before processing starts."""
        pass

    def doDispose(self) -> None:
        """Cleanup resources. Called once after processing ends."""
        pass

    def __dispose__(self):
        """Clean up worker resources."""
        self.__exec__(self.doDispose)
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
        return self.__exec__(self.doTask, inp)
