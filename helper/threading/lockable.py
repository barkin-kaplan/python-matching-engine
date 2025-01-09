import threading
from typing import Generic, TypeVar

T = TypeVar("T")


class Lockable(Generic[T]):
    def __init__(self, value: T):
        self._value: T = value
        self._lock: threading.Lock = threading.Lock()

    def acquire(self):
        self._lock.acquire()

    def release(self):
        self._lock.release()

    def set_value(self, new_value: T):
        self._value = new_value

    def get_value(self) -> T:
        return self._value

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
