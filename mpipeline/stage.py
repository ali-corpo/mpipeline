from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Generic
from typing import Literal
from typing import TypeVar

from .worker import Worker


T = TypeVar('T')
Q = TypeVar('Q')


@dataclass
class Stage(Generic[T, Q]):
    worker_class: type[Worker[T, Q]]
    worker_count: int = 1
    # queue_size: int = 100
    mode: Literal['thread', 'process'] = 'thread'
    multiprocess_mode: Literal['spawn', 'fork'] = 'spawn'
    worker_args: tuple = field(default_factory=tuple)
    worker_kwargs: dict = field(default_factory=dict)
