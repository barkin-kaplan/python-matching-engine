from helper.logging.bk_logging import IBKLogger
from helper.logging.i_bk_logging import IBKLogging
from helper.logging.mock_bk_logger import MockBKLogger


class MockBKLogging(IBKLogging):
    def get_logger_console(self, logger_name) -> IBKLogger:
        return MockBKLogger()

    def get_logger_file(self, logger_name, leave_stream_open: bool = False) -> IBKLogger:
        return MockBKLogger()

    def get_logger_hybrid(self, logger_name, leave_stream_open: bool = False) -> IBKLogger:
        return MockBKLogger()
