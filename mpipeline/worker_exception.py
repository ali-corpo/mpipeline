from multiprocessing.managers import DictProxy
from typing import Any
from typing import Generic
from typing import TypeVar

import tblib.pickling_support


tblib.pickling_support.install()

T = TypeVar('T')


class WorkerException(Exception, Generic[T]):
    stage: Any
    work_item: T | None
    tb_frame: Any

    def __init__(self, orig_exc: BaseException, stage: Any, work_item: T | None, shared_data: DictProxy | None):
        super().__init__(orig_exc, str(stage), work_item, shared_data)
        self.tb_frame = orig_exc.__traceback__
        self.orig_exc = orig_exc
        self.work_item = work_item
        self.stage = str(stage)
        self.shared_data = shared_data

    def __str__(self):
        return f"WorkerException {self.orig_exc}\n\t\tstage: {self.stage}\n\t\twork_item: {self.work_item}\n\t\tshared_data: {self.shared_data}"

    def re_raise(self):
        raise self.with_traceback(self.tb_frame)
