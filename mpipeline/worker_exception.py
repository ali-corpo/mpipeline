import pickle
from multiprocessing.managers import DictProxy
from multiprocessing.managers import ListProxy
from typing import Any
from typing import Generic
from typing import TypeVar

from tblib import Traceback


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
    tb_frame: dict | None

    def __init__(self, orig_exc: BaseException, stage: Any, work_item: T | None, shared_data: dict | None, tb_frame: dict | None = None):
        shared_data2 = to_dict_recursive(shared_data)
        try:
            pickle.dumps(shared_data2)
            self.shared_data = shared_data2
        except BaseException:
            self.shared_data = None
        try:
            pickle.dumps(work_item)
            self.work_item = work_item
        except BaseException:
            self.work_item = None
        self.stage = str(stage)

        self.tb_frame = tb_frame or Traceback(orig_exc.__traceback__).as_dict()

        self.orig_exc = orig_exc

        super().__init__(self.orig_exc, self.stage, self.work_item, self.shared_data, self.tb_frame)

    def __str__(self):
        return f"{type(self.orig_exc)} {self.orig_exc}\n\t\tstage: {self.stage}\n\t\twork_item: {self.work_item}\n\t\tshared_data: {self.shared_data}"

    def re_raise(self):
        if self.tb_frame is None:
            raise self
        # traceback.print_tb(Traceback.from_dict(self.tb_frame).as_traceback())
        raise self.with_traceback(Traceback.from_dict(self.tb_frame).as_traceback())
