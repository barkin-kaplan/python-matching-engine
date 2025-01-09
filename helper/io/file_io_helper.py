import datetime
import os
import random
import time
import traceback
from dataclasses import dataclass
from enum import Enum
from collections import deque
from urllib.parse import quote


import requests


from helper.ioc_container import IOCContainer
from helper.threading.bk_thread_helper import BKThreadHelper




class FileIOHelper:
   class __WriteType(Enum):
       OVERWRITE = 0
       APPEND_LINE = 2


   @dataclass
   class __AppendQueueItem:


       file_name: str
       message: str
       leave_stream_open: bool
       write_type: Enum


   def __init__(self, start_background=True):
       self.__streams: dict = dict()
       self.__log_queue: deque = deque()
       self._last_stream_close_time = datetime.datetime.now()
       self._thread_helper = IOCContainer.get_instance_singleton(BKThreadHelper)
       self._thread_helper.thread_start(self.__file_write_thread, start_background, thread_name="file_io_helper_thread")


   def __file_write_thread(self):
       while True:
           try:
               if len(self.__log_queue) > 0:
                   queue_item: FileIOHelper.__AppendQueueItem = self.__log_queue.popleft()
                   if queue_item.write_type == FileIOHelper.__WriteType.APPEND_LINE:
                       if queue_item.leave_stream_open:
                           if queue_item.file_name not in self.__streams:
                               stream = open(queue_item.file_name, "a", encoding="utf-8")
                               self.__streams[queue_item.file_name] = stream
                           else:
                               stream = self.__streams[queue_item.file_name]
                           stream.write(queue_item.message + "\n")
                       else:
                           stream = open(queue_item.file_name, "a", encoding="utf-8")
                           stream.write(queue_item.message + "\n")
                           stream.close()
                   elif queue_item.write_type == FileIOHelper.__WriteType.OVERWRITE:
                       stream = open(queue_item.file_name, "w", encoding="utf-8")
                       stream.write(queue_item.message)
               # close streams on day advance
               now = datetime.datetime.now()
               if now.day != self._last_stream_close_time.day:
                   for stream in self.__streams.values():
                       stream.close()
                   self.__streams.clear()
                   self._last_stream_close_time = now
               else:
                   time.sleep(0.01)
           except:
               print(f"Error in file write thread {traceback.format_exc()}")


   def append_line(self, message: str, file_name: str, leave_stream_open: bool):
       queue_item = FileIOHelper.__AppendQueueItem(file_name, message, leave_stream_open, FileIOHelper.__WriteType.APPEND_LINE)
       self.__log_queue.append(queue_item)


   def overwrite_text(self, filepath, content):
       queue_item = FileIOHelper.__AppendQueueItem(filepath, content, False, FileIOHelper.__WriteType.OVERWRITE)
       self.__log_queue.append(queue_item)



