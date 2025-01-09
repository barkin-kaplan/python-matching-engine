import time
import traceback
from dataclasses import dataclass
from threading import Lock
from typing import Dict

import requests
import psutil

from helper import bk_time
from helper.logging.bk_logging import BKLogging
from helper.ioc_container import IOCContainer
from helper.notification.telegram_connector import TelegramConnector
from helper.threading.bk_thread_helper import BKThreadHelper

import urllib3


class HealthCheckNotifier:

    @dataclass
    class __NotificationModel:
        last_healthy_time: float
        alert_interval_seconds: int
        name: str
        last_error_time: float

    def __init__(self, health_checker_url: str, health_checker_api_key: str, service_name: str, use_health_checker: bool):
        try:
            self._telegram_connector: TelegramConnector = IOCContainer.get_instance_singleton(TelegramConnector)
        except:
            self._telegram_connector = None
        self._notification_models: Dict[str, HealthCheckNotifier.__NotificationModel] = {}
        logger_factory: BKLogging = IOCContainer.get_instance_singleton(BKLogging)
        self._hybrid_logger_health_check = logger_factory.get_logger_hybrid(f"{type(self).__name__}_healthcheck")
        self._hybrid_logger_exception = logger_factory.get_logger_hybrid(f"{type(self).__name__}_exception")
        self._lock = Lock()
        self._health_checker_url = health_checker_url
        self._health_checker_api_key = health_checker_api_key
        self._service_name = service_name
        self._use_health_checker = use_health_checker
        self._thread_helper: BKThreadHelper = IOCContainer.get_instance_singleton(BKThreadHelper)
        urllib3.disable_warnings()
        self._thread_helper.thread_start(self._notification_check_thread, True, thread_name="thread_health_check_notification_thread")

    def _notification_check_thread(self):
        while True:
            try:
                with self._lock:
                    disk_usage = psutil.disk_usage('/').percent
                    memory_usage = psutil.virtual_memory().percent
                    cpu_usage = psutil.cpu_percent(interval=2)
                    thread_model_statuses = {}
                    for notification_model in list(self._notification_models.values()):
                        now = time.time()
                        diff_seconds = now - notification_model.last_healthy_time
                        if diff_seconds > notification_model.alert_interval_seconds:
                            thread_model_statuses[notification_model.name] = False
                            message = f"Thread health check failed {notification_model.name}"
                            if self._telegram_connector:
                                self._telegram_connector.send_message_with_retry(message)
                            self._hybrid_logger_health_check.log_info(message)
                        else:
                            thread_model_statuses[notification_model.name] = True
                    if self._use_health_checker:
                        headers = {"api_key": self._health_checker_api_key}
                        data = {"service_name": self._service_name,
                                "data": thread_model_statuses,
                                "usage": {
                                        "disk": str(disk_usage),
                                        "memory": str(memory_usage),
                                        "cpu": str(cpu_usage)
                                    }
                                }
                        response = requests.post(self._health_checker_url, headers=headers, json=data, timeout=5, verify=False)
            except:
                self._hybrid_logger_exception.log_error(traceback.format_exc())
            time.sleep(18)

    def register(self, name: str, alert_interval_seconds: int):
        if name in self._notification_models:
            raise Exception(f"Thread name is already registered {name}")
        self._notification_models[name] = self.__NotificationModel(time.time(), alert_interval_seconds, name, 0)

    def unregister(self, name: str):
        if name in self._notification_models:
            del self._notification_models[name]

    def set_healthy_status(self, name: str):
        if name in self._notification_models:
            self._notification_models[name].last_healthy_time = time.time()

    def notify_error_status(self, name: str):
        if not self._telegram_connector:
            return
        if name in self._notification_models:
            notification_model = self._notification_models[name]
            error_diff = time.time() - notification_model.last_error_time
            if error_diff > 5:
                self._telegram_connector.send_message_with_retry(f"Error occurred in thread {name}")
                notification_model.last_error_time = time.time()

