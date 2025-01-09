
from typing import Optional
from helper.logging.bk_logging import BKLogging, IBKLogger
from helper.ioc_container import IOCContainer
from helper.notification.telegram_connector import TelegramConnector
from helper.threading.bk_thread_helper import BKThreadHelper
from helper.threading.health_check_notifier import HealthCheckNotifier


class BLWorker:
    def __init__(self, thread_helper = False, health_checker = False, logger_factory = False, telegram_connector = False, default_loggers = False) -> None:
        self.thread_helper: BKThreadHelper = IOCContainer.get_instance_singleton(BKThreadHelper) if thread_helper else None # type: ignore
        self.health_checker: HealthCheckNotifier = IOCContainer.get_instance_singleton(HealthCheckNotifier) if health_checker else None # type: ignore
        self.logger_factory: BKLogging = IOCContainer.get_instance_singleton(BKLogging) if logger_factory else None # type: ignore
        self.telegram_connector: TelegramConnector = IOCContainer.get_instance_singleton(TelegramConnector) if telegram_connector else None # type: ignore

        if default_loggers:
            logger_factory = IOCContainer.get_instance_singleton(BKLogging)
            self.event_logger: IBKLogger = logger_factory.get_logger_hybrid(f"{type(self).__name__}_event") # type: ignore
            self.exception_logger: IBKLogger = logger_factory.get_logger_hybrid(f"{type(self).__name__}_exception") # type: ignore
            self.debug_logger: IBKLogger = logger_factory.get_logger_hybrid(f"{type(self).__name__}_debug") # type: ignore
