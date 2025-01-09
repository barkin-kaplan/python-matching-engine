import datetime
import os.path
import random
import threading
import time
import traceback
from typing import Any, Callable, Dict
from urllib.parse import quote


import requests


from helper import bk_time
from helper.serialization.json import json_encoding
_prod_env = "production"
_staging_env = "staging"
_envs = {_prod_env, _staging_env}
class BKThreadHelper:
   def __init__(self, crash_dump_root: str):
       self._bilira_env = os.environ.get("BILIRA_ENV")
       if self._bilira_env not in _envs:
           self.dump_root = crash_dump_root
           if not os.path.exists(self.dump_root):
               os.makedirs(self.dump_root)
       self._thread_counts: Dict[str, int] = {}
       self._thread_count_lock = threading.Lock()
       self._thread_count_file_name = "thread_counts"
       if self._bilira_env not in _envs:
           self.thread_start(self._thread_count_dump_thread, True, thread_name="thread_count_dump_thread")


   @property
   def _thread_count_file_path(self):
       return self.dump_root + "/" + self._thread_count_file_name


   def _thread_count_dump_thread(self):
       while True:
           try:
               json_s = json_encoding.encode(self._thread_counts)


               if os.path.exists(self._thread_count_file_path):
                   file_size = os.path.getsize(self._thread_count_file_path)
               else:
                   file_size = 0
               if file_size > 40000000:
                   file_open_mode = "w"
               else:
                   file_open_mode = "a"
               with open(self._thread_count_file_path, file_open_mode) as f:
                   f.write(bk_time.get_utc_now_micro_formatted() + " : " + json_s + "\n")
               time.sleep(30)
           except:
               with open(self._thread_count_file_path, "a") as f:
                   f.write(f"error in thread count write {traceback.format_exc()}\n")


   def __thread_encapsulate(self, func: Callable[..., Any], thread_name: str, *args: Any, **kwargs: Any):
       try:
           with self._thread_count_lock:
               if thread_name not in self._thread_counts:
                   self._thread_counts[thread_name] = 0
               self._thread_counts[thread_name] += 1
           func(*args, **kwargs)
           with self._thread_count_lock:
               self._thread_counts[thread_name] -= 1
       except:
           time.sleep(1)
           with self._thread_count_lock:
               self._thread_counts[thread_name] -= 1
           now_s = datetime.datetime.now().strftime(bk_time.micro_format).replace(":", "-")
           if self._bilira_env not in _envs:
               if not os.path.exists(self.dump_root):
                   os.makedirs(self.dump_root)
               with open(f"{self.dump_root}/{now_s}", "w") as f:
                   f.write(traceback.format_exc())


   def thread_start(self, func: Callable[..., Any], is_background: bool, *args: Any, thread_name: str = "unknown thread", **kwargs: Any) -> threading.Thread:
       thread = threading.Thread(target=self.__thread_encapsulate, args=(func, thread_name, *args), kwargs=kwargs, name=thread_name)
       thread.daemon = is_background  # Daemonize thread
       thread.start()
       return thread





