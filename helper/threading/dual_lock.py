from threading import Lock

class DualLock:
    def __init__(self, lock_a: Lock, lock_b: Lock):
        self.lock_a = lock_a
        self.lock_b = lock_b

    def __enter__(self):
        self.lock_a.acquire()
        self.lock_b.acquire()

    def __exit__(self, exc_type, exc_value, traceback):
        self.lock_b.release()
        self.lock_a.release()
