from helper.logging.bk_logging import IBKLogger


class MockBKLogger(IBKLogger):
    def log_info(self, message: str):
        print(f"[INFO]\t{message}")

    def log_debug(self, message: str):
        print(f"[DEBUG]\t{message}")

    def log_error(self, message: str):
        print(f"[ERROR]\t{message}")

    def log_plain(self, message: str):
        print(f"{message}")
