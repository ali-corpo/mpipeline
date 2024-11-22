from multiprocessing.managers import DictProxy
from multiprocessing.managers import ListProxy
from typing import Any
from typing import Generic
from typing import TypeVar

import tblib.pickling_support


tblib.pickling_support.install()

T = TypeVar('T')


def to_dict_recursive(obj):
    if isinstance(obj, dict) or isinstance(obj, DictProxy):
        return {k: to_dict_recursive(v) for k, v in obj.items()}
    elif isinstance(obj, list) or isinstance(obj, ListProxy):
        return [to_dict_recursive(v) for v in obj]
    else:
        return obj


class WorkerException(Exception, Generic[T]):
    stage: Any
    work_item: T | None
    tb_frame: Any

    def __init__(self, orig_exc: BaseException, stage: Any, work_item: T | None, shared_data: DictProxy | None):
        if shared_data is not None:
            self.shared_data = to_dict_recursive(shared_data)
        super().__init__(orig_exc, str(stage), work_item, self.shared_data)
        self.tb_frame = orig_exc.__traceback__
        self.orig_exc = orig_exc
        self.work_item = work_item
        self.stage = str(stage)

    def __str__(self):
        return f"WorkerException {self.orig_exc}\n\t\tstage: {self.stage}\n\t\twork_item: {self.work_item}\n\t\tshared_data: {self.shared_data}"

    def re_raise(self):
        raise self.with_traceback(self.tb_frame)
