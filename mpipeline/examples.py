from __future__ import annotations
import abc
import asyncio
import multiprocessing as mp
from dataclasses import dataclass
from typing import Generic, TypeVar, Callable, Iterator, AsyncIterator, Optional, Type, List, Any, Tuple, Iterable
from queue import Empty
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import threading
from multiprocessing.context import SpawnContext
from multiprocessing.pool import ThreadPool
import time
import random
import heapq
from tqdm.auto import tqdm
import asyncio
from time import perf_counter
from collections import deque
import sys
import warnings
from .worker import Worker
# Example usage:
import random


class NumberGenerator(Worker[int, int]):
    """Generates numbers and simulates some processing."""

    def doTask(self, inp: int) -> int:
        time.sleep(0.1+random.random()/10)  # Simulate work
        return inp * 2
    def doDispose(self):
        print('Dispose NumberGenerator worker')
        pass


class SlowProcessor(Worker[int, float]):
    """Processes numbers with varying delays."""

    def doTask(self, inp: int) -> float:
        time.sleep(0.2+random.random()/10)  # Simulate longer processing
        return inp * 1.5


class ErrorProneWorker(Worker[float, str]):
    """Demonstrates error handling."""

    def doTask(self, inp: float) -> str:
        if inp > 20:
            raise ValueError(f"Input too large: {inp}")
        return f"Processed: {inp:.1f}"
