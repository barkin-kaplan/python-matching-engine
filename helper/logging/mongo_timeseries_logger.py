
from datetime import datetime
from queue import Queue
from threading import Lock
import time
import traceback
from typing import Dict, Tuple
from pymongo import MongoClient
import pymongo
from pymongo.errors import CollectionInvalid
import pytz
from helper.logging.bk_logging import BKLogging, IBKLogger

from helper.ioc_container import IOCContainer
from helper.threading.bk_thread_helper import BKThreadHelper

class MongoLogger:

    TIMESTAMP_ATTR_NAME: str = "timestamp"
    _LOG_EXPIRATION_SECONDS: int = 7*24*60*60 # 1 week
    _READ_INTERVAL_SECONDS: int = 5
    _EXCEPTION_WAIT_SECONDS: int = 5

    def __init__(self, url: str, db_name: str, collection_name: str, read_start_time: datetime=None, tls: bool=True) -> None:
        self.url = url
        self.tls = tls
        self._client: MongoClient = MongoClient(url, tls=tls)
        self._db_name: str = db_name
        self._collection_name: str = collection_name
        self._istanbul_timezone = pytz.timezone('Europe/Istanbul')
        self._read_start_time = read_start_time
        self._read_queue = Queue()
        self._write_queue = Queue()
        self._read_lock = Lock()
        self._write_lock = Lock()
        self._reading = False
        self._writing = False
        self._hybrid_logger: IBKLogger = IOCContainer.get_instance_singleton(BKLogging).get_logger_hybrid(f"{type(self).__name__}")
        self._thread_helper: BKThreadHelper = IOCContainer.get_instance_singleton(BKThreadHelper)
        self._create_collection()

    def _create_collection(self):
        try:
            collection_options = {
                "timeseries": {"timeField": MongoLogger.TIMESTAMP_ATTR_NAME}
                }
            self._client[self._db_name].create_collection(self._collection_name, **collection_options)
            # self._client[self._db_name][self._collection_name].create_index([("timestamp", pymongo.ASCENDING)], expireAfterSeconds=self._LOG_EXPIRATION_SECONDS)
        except CollectionInvalid:
            pass # Collection already exists.

    def write_log(self, message: str):
        with self._write_lock:
            if not self._writing:
                self._writing = True
                self._thread_helper.thread_start(self._write_thread, is_background=True, thread_name="mongo_write_log_thread")
        timestamp = datetime.now(self._istanbul_timezone)
        self._write_queue.put((timestamp, message))

    def _write_thread(self):
        while self._writing:
            try:
                timestamp, message = self._write_queue.get()
                while self._writing:
                    try:
                        self._client[self._db_name][self._collection_name].insert_one({"timestamp": timestamp, "message": message})
                        break
                    except:
                        time.sleep(self._EXCEPTION_WAIT_SECONDS)
            except:
                self._hybrid_logger.log_error(f"Error in Mongo write thread:\n{traceback.format_exc()}")

    def read_log(self):
        with self._read_lock:
            if not self._reading:
                self._reading = True
                self._last_read = self._read_start_time if self._read_start_time else datetime.now(self._istanbul_timezone)
                self._thread_helper.thread_start(self._read_thread, is_background=True, thread_name="mongo_read_log_thread")
        return self._read_queue.get()

    def _read_thread(self):
        while self._reading:
            try:
                now = datetime.now(self._istanbul_timezone)
                logs = None
                try:
                    logs = self._client[self._db_name][self._collection_name].find({"timestamp": {"$gte": self._last_read, "$lte": now}})
                except:
                    time.sleep(self._EXCEPTION_WAIT_SECONDS)
                self._last_read = now
                if logs:
                    for l in logs:
                        self._read_queue.put((l["timestamp"], l["message"]))
            except:
                self._hybrid_logger.log_error(f"Error in Mongo read thread:\n{traceback.format_exc()}")
            time.sleep(self._READ_INTERVAL_SECONDS)

    def read_direct(self, query: Dict) -> Tuple[datetime, str]:
        logs = self._client[self._db_name][self._collection_name].find(query)
        return [(l["timestamp"], l["message"]) for l in logs]

    def set_read_start_time(self, read_start_time: datetime):
        if not self._reading:
            self._read_start_time = read_start_time

    def stop(self):
        self._reading = False
        self._writing = False