from __future__ import annotations

import random
import threading
import time

from .worker import Worker
from mpipeline.thread_safe_dict import ThreadSafeDict
# Example usage:


class NumberGenerator(Worker[int, float]):
    """Generates numbers and simulates some processing."""

    def __init__(self, name: str = ""):
        self.name = name
        time.sleep(3)

    def doTask(self, inp: int, **kwargs) -> int:
        time.sleep(0.1 + random.random() / 10)  # Simulate work
        return inp * 2

    def doDispose(self):
        print('Dispose NumberGenerator worker', threading.current_thread().name)


class SlowProcessor(Worker[float, float]):
    """Processes numbers with varying delays."""

    def __init__(self, name: str = ""):
        self.name = name
        time.sleep(3)

    def doTask(self, inp: float, **kwargs) -> float:
        time.sleep(0.2 + random.random() / 10)  # Simulate longer processing
        return inp * 1.5


class SharedDataProcessor(Worker[float, float]):
    """Processes numbers with varying delays."""

    def __init__(self, name: str = ""):
        self.name = name
        time.sleep(3)

    def doTask(self, inp: float, *, thread_mode_shared_data: ThreadSafeDict, **kwargs) -> float:
        thread_mode_shared_data.get_or_set('count', lambda: 0)
        thread_mode_shared_data['count'] += 1
        time.sleep(0.2 + random.random() / 10)  # Simulate longer processing
        print("count:", thread_mode_shared_data['count'])
        return inp * 1.5


class ErrorProneWorker(Worker[float, str]):
    """Demonstrates error handling."""

    def __init__(self, name: str = ""):
        self.name = name

    def doTask(self, inp: float, **kwargs) -> str:
        if inp > 20:
            raise ValueError(f"Input too large: {inp}")
        return f"Processed: {inp:.1f}"
