from collections import defaultdict
from threading import RLock


class ThreadSafeDict(defaultdict):
    def __init__(self, default_factory=None):
        super().__init__(default_factory or ThreadSafeDict)
        self._lock = RLock()

    def __setitem__(self, key, value):
        """
        Set a value in the dictionary. Automatically wraps nested dictionaries as ThreadSafeDict.
        """
        if isinstance(value, dict) and not isinstance(value, ThreadSafeDict):
            value = ThreadSafeDict.from_dict(value)
        super().__setitem__(key, value)

    def get_or_set(self, key, func):
        """
        Gets the value for the key or sets it using the provided function if it doesn't exist.
        :param key: The key to check or set.
        :param func: A function that generates the value to set.
        :return: The existing or newly set value for the key.
        """
        if key in self:
            return self[key]

        with self._lock:
            # Double-check locking to ensure the key is still absent after acquiring the lock.
            if key not in self:
                self[key] = func()

        return self[key]

    @staticmethod
    def from_dict(d):
        """
        Recursively converts a regular dictionary to a ThreadSafeDict.
        :param d: A regular dictionary to convert.
        :return: A ThreadSafeDict containing the same data.
        """
        tsd = ThreadSafeDict()
        for key, value in d.items():
            tsd[key] = ThreadSafeDict.from_dict(value) if isinstance(value, dict) else value
        return tsd

    def __repr__(self):
        """
        String representation of the ThreadSafeDict.
        """
        return f"{self.__class__.__name__}({super().__repr__()})"
