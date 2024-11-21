from __future__ import annotations

import random
import time
from multiprocessing.managers import DictProxy

from .worker import Worker
# Example usage:


class NumberGenerator(Worker[int, float]):
    """Generates numbers and simulates some processing."""

    def doTask(self, inp: int, shared_data: DictProxy, **kwargs) -> int:
        time.sleep(0.1 + random.random() / 10)  # Simulate work
        return inp * 2

    def doDispose(self):
        print('Dispose NumberGenerator worker')


class SlowProcessor(Worker[float, float]):
    """Processes numbers with varying delays."""

    def __init__(self, name: str = ""):
        self.name = name

    def doTask(self, inp: float, shared_data: DictProxy, **kwargs) -> float:
        time.sleep(0.2 + random.random() / 10)  # Simulate longer processing
        return inp * 1.5


class ErrorProneWorker(Worker[float, str]):
    """Demonstrates error handling."""

    def __init__(self, name: str = ""):
        self.name = name

    def doTask(self, inp: float, shared_data: DictProxy, **kwargs) -> str:
        if inp > 20:
            raise ValueError(f"Input too large: {inp}")
        return f"Processed: {inp:.1f}"
