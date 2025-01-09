from threading import Lock
from typing import Dict, TypeVar, Generic

K = TypeVar("K")
V = TypeVar("V")


class BKDict(Dict[K, V]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._lock = Lock()

    def set(self, key, value):
        with self._lock:
            self[key] = value

    def try_get_or_set(self, key, new_value) -> V:
        """In an atomic operation tries to get value specified by key. If key is not present sets new_value under key 'key'. Returns ('existing value', True) if key
        already exists, returns ('new value', False) otherwise"""
        with self._lock:
            if key in self:
                return self[key]
            self[key] = new_value
            return new_value

    def try_remove(self, key):
        with self._lock:
            if key in self:
                del self[key]






