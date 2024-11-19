from __future__ import annotations
import multiprocessing as mp
from dataclasses import dataclass, field
from typing import Generic, TypeVar, Type

from .worker import Worker
# Default to spawn context
DEFAULT_MP_CONTEXT = 'spawn'

# Use spawn context globally
mp_ctx = mp.get_context(DEFAULT_MP_CONTEXT)

# Thread-local storage for worker instances


T = TypeVar('T')
Q = TypeVar('Q')



@dataclass
class Stage(Generic[T, Q]):
    worker_class: Type[Worker[T, Q]]
    worker_count: int = 1
    queue_size: int = 100
    use_threads: bool = False
    mp_context: str = DEFAULT_MP_CONTEXT
    worker_args: tuple = field(default_factory=tuple)
    worker_kwargs: dict = field(default_factory=dict)

