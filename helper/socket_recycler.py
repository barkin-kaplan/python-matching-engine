import time
import traceback

from helper import psutil_helper
from helper.bk_logging import IBKLogger, BKLogging
from helper.ioc_container import IOCContainer
from helper.threading.bk_thread_helper import BKThreadHelper
from helper.threading.health_check_notifier import HealthCheckNotifier


class SocketRecycler:
    def __init__(self, use_health_checker: bool = True):
        self._use_health_checker = use_health_checker
        if use_health_checker:
            self._health_check_notifier: HealthCheckNotifier = IOCContainer.get_instance_singleton(HealthCheckNotifier)
        self._thread_helper: BKThreadHelper = IOCContainer.get_instance_singleton(BKThreadHelper)
        logger_factory: BKLogging = IOCContainer.get_instance_singleton(BKLogging)
        self._hybrid_logger_info: IBKLogger = logger_factory.get_logger_hybrid(f"{type(self).__name__}_info")
        self._hybrid_logger_exception: IBKLogger = logger_factory.get_logger_hybrid(f"{type(self).__name__}_exception")
        self._file_logger_exception: IBKLogger = logger_factory.get_logger_file(f"{type(self).__name__}_exception")
        self._thread_name = f"SocketRecycler"
        # connection_key -> timestamp
        self._connection_records: dict = {}

    @staticmethod
    def _get_key(connection):
        return f"{connection.laddr.ip}:{connection.laddr.port}->{connection.raddr.ip}:{connection.raddr.port}"

    def _register_and_start_thread(self):
        if self._use_health_checker:
            self._health_check_notifier.register(self._thread_name, 60)
            self._thread_helper.thread_start(self._thread_func, is_background=True, thread_name=self._thread_name)

    def _thread_func(self):
        self._hybrid_logger_info.log_info("Started")
        while True:
            try:
                if self._use_health_checker:
                    self._health_check_notifier.set_healthy_status(self._thread_name)
                try:
                    close_wait_connections = psutil_helper.get_close_wait_sockets_of_current_process_for_ubuntu()
                except:
                    self._file_logger_exception.log_error(f"Error getting socket connections using ubuntu func {traceback.format_exc()}")
                    close_wait_connections = psutil_helper.get_close_wait_sockets_of_current_process_for_mac()
                expired_connections = []
                connection_keys: set = {self._get_key(connection) for connection in close_wait_connections}
                # remove connections that are not in CLOSE_WAIT status
                for connection_key in list(self._connection_records.keys()):
                    if connection_key not in connection_keys:
                        self._hybrid_logger_info.log_info(f"Removing connection  because it is no longer in CLOSE_WAIT key {connection_key}")
                        del self._connection_records[connection_key]
                for connection in close_wait_connections:
                    key = self._get_key(connection)
                    if key not in self._connection_records:
                        self._hybrid_logger_info.log_info(f"Added close wait connection record {key}")
                        self._connection_records[key] = time.time()
                    else:
                        timestamp = self._connection_records[key]
                        if time.time() - timestamp > 60:
                            expired_connections.append(connection)

                for connection in expired_connections:
                    key = self._get_key(connection)
                    self._hybrid_logger_info.log_info(f"Shutting down connection {key}")
                    psutil_helper.shutdown_connections([connection])
                    del self._connection_records[key]
            except:
                self._hybrid_logger_exception.log_error(f"{traceback.format_exc()}")
            time.sleep(20)

    def start(self):
        self._register_and_start_thread()
