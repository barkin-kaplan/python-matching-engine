import time
from dataclasses import dataclass
from collections import deque

from helper.ioc_container import IOCContainer
from helper.threading.bk_thread_helper import BKThreadHelper


class ConsoleIOHelper:
    @dataclass
    class __QueueItem:
        message: str

    def __init__(self, start_background=True):
        self.__queue = deque()
        self._thread_helper: BKThreadHelper = IOCContainer.get_instance_singleton(BKThreadHelper)
        self._thread_helper.thread_start(self.__console_thread, start_background, thread_name="console_io_helper_thread")

    def __console_thread(self):
        while True:
            if len(self.__queue) > 0:
                item = self.__queue.popleft()
                print(item.message)
            else:
                time.sleep(0.01)

    def console_write(self, message: str):
        item = ConsoleIOHelper.__QueueItem(message)
        self.__queue.append(item)
