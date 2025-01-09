import time
import traceback
from threading import Lock
from typing import Dict, Optional, List, Generator

import pymongo

from helper.bk_logging import IBKLogger, BKLogging
from helper.ioc_container import IOCContainer
from helper.threading.bk_thread_helper import BKThreadHelper
from helper.threading.health_check_notifier import HealthCheckNotifier


class MongoDbConnector:
    __UPDATE = "update"
    __INSERT = "insert"
    __UPSERT = "upsert"

    class __WriteQueueItem:
        def __init__(self, collection_name, document: dict, operation_type: str, update_filter: dict):
            self.collection_name = collection_name
            self.data = document
            self.operation_type = operation_type
            self.update_filter = update_filter

    def __init__(self, connection_string: str, db_name: str):
        logger_factory: BKLogging = IOCContainer.get_instance_singleton(BKLogging)
        self._hybrid_logger_exception: IBKLogger = logger_factory.get_logger_hybrid(f"{type(self).__name__}_{db_name}_exception")
        self._thread_helper: BKThreadHelper = IOCContainer.get_instance_singleton(BKThreadHelper)
        self._health_check_notifier: HealthCheckNotifier = IOCContainer.get_instance_singleton(HealthCheckNotifier)
        self._write_thread_name = f"{type(self).__name__}_{db_name}_write_thread"
        self._client = pymongo.MongoClient(connection_string)
        self._db = self._client[db_name]
        self._write_list: List[MongoDbConnector.__WriteQueueItem] = []
        self._write_lock = Lock()
        self._register_and_start_threads()

    def _register_and_start_threads(self):
        self._health_check_notifier.register(self._write_thread_name, 20)
        self._thread_helper.thread_start(self._write_thread, is_background=False, thread_name=self._write_thread_name)

    def _write_thread(self):
        while True:
            try:
                self._health_check_notifier.set_healthy_status(self._write_thread_name)
                with self._write_lock:
                    if len(self._write_list) > 0:
                        list_copy = [o for o in self._write_list]
                        self._write_list.clear()
                        batched_insert_items = {}
                        updates = []
                        upserts = []
                        for item in list_copy:
                            collection_name = item.collection_name
                            if item.operation_type == MongoDbConnector.__INSERT:
                                if collection_name not in batched_insert_items:
                                    batched_insert_items[collection_name] = []
                                batched_insert_items[collection_name].append(item)
                            elif item.operation_type == MongoDbConnector.__UPDATE:
                                # do updates after insert
                                updates.append(item)
                            elif item.operation_type == MongoDbConnector.__UPSERT:
                                upserts.append(item)
                        for collection_name in batched_insert_items:
                            try:
                                items = [o.data for o in batched_insert_items[collection_name]]
                                self._db[collection_name].insert_many(items)
                            except:
                                self._write_list += batched_insert_items[collection_name]
                                self._hybrid_logger_exception.log_error(f"Error Re-appended insert items. {traceback.format_exc()}")
                        for upsert in upserts:
                            try:
                                self._db[upsert.collection_name].update_one(upsert.update_filter, {"$set": upsert.data}, upsert=True)
                            except:
                                self._hybrid_logger_exception.log_error(f"Error upserting item. Re-appending to write list. {traceback.format_exc()}")
                                self._write_list.append(upsert)
                        for update_item in updates:
                            try:
                                self._db[update_item.collection_name].update_one(update_item.update_filter, {"$set": update_item.data})
                            except:
                                self._hybrid_logger_exception.log_error(f"Error updating item. Re-appending to write list. {traceback.format_exc()}")
                                self._write_list.append(update_item)
            except:
                self._hybrid_logger_exception.log_error(f"Error in write thread: {traceback.format_exc()}")
                self._health_check_notifier.notify_error_status(self._write_thread_name)
            time.sleep(0.01)

    def set_unique_index(self, collection_name: str, field_name):
        collection = self._db[collection_name]
        collection.create_index(field_name, unique=True)

    def set_index(self, collection_name: str, field_name):
        collection = self._db[collection_name]
        collection.create_index(field_name)

    def save_record(self, collection_name: str, document: dict):
        self._db[collection_name].insert_one(document)

    def save_record_parallel(self, collection_name: str, document: dict):
        with self._write_lock:
            self._write_list.append(MongoDbConnector.__WriteQueueItem(collection_name, document, MongoDbConnector.__INSERT, {}))

    def update_record(self, collection_name: str, document: dict, update_filter: dict):
        self._db[collection_name].update_one(update_filter, document)

    def update_record_parallel(self, collection_name: str, document: dict, update_filter: dict):
        self._write_list.append(MongoDbConnector.__WriteQueueItem(collection_name, document, MongoDbConnector.__UPDATE, update_filter))

    def upsert_record(self, collection_name: str, document: dict, update_filter: dict):
        self._db[collection_name].update_one(update_filter, {"$set": document}, upsert=True)

    def upsert_record_parallel(self, collection_name: str, document: dict, update_filter: dict):
        self._write_list.append(MongoDbConnector.__WriteQueueItem(collection_name, document, MongoDbConnector.__UPSERT, update_filter))

    def read_records(self, collection_name, query: Optional[Dict] = None) -> Generator[dict, None, None]:
        if query is None:
            query = {}
        cursor = self._db[collection_name].find(query)
        for item in cursor:
            yield item

    def read_one(self, collection_name: str, query: Optional[Dict] = None) -> dict:
        if query is None:
            query = {}
        result = self._db[collection_name].find_one(query)
        return result

    def get_collection(self, collection_name: str):
        return self._db[collection_name]
