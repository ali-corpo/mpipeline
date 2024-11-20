

from typing import Any, Generic, TypeVar
import tblib.pickling_support
tblib.pickling_support.install()

T = TypeVar('T')


class WorkerException(Exception, Generic[T]):
    stage: Any
    work_item: T | None
    tb_frame: Any

    def __init__(self, orig_exc: BaseException, stage: Any, work_item: T | None):
        super().__init__(orig_exc, str(stage), work_item)
        self.tb_frame = orig_exc.__traceback__
        self.orig_exc = orig_exc
        self.work_item = work_item
        self.stage = str(stage)

    def __str__(self):
        return f"WorkerException {self.stage}, {self.work_item} --> {self.orig_exc}"

    def re_raise(self):
        raise self.with_traceback(self.tb_frame)
