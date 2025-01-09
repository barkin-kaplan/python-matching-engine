from abc import abstractmethod

from helper.logging.bk_logging import IBKLogger


class IBKLogging:
    @abstractmethod
    def get_logger_console(self, logger_name) -> IBKLogger:
        pass

    @abstractmethod
    def get_logger_file(self, logger_name, leave_stream_open: bool = False) -> IBKLogger:
        pass

    @abstractmethod
    def get_logger_hybrid(self, logger_name, leave_stream_open: bool = False) -> IBKLogger:
        pass




