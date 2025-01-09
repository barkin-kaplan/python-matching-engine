import datetime
import inspect
import logging
import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from sys import path
from typing import List, Dict, Optional
import os
from collections import deque




from helper import bk_time, misc_helper


from helper.io.console_io_helper import ConsoleIOHelper
from helper.io.file_io_helper import FileIOHelper
from helper.ioc_container import IOCContainer
from helper.serialization.json import json_encoding
from helper.threading.bk_thread_helper import BKThreadHelper




class IBKLogger(ABC):
    @abstractmethod
    def log_info(self, message: str):
        pass


    @abstractmethod
    def log_debug(self, message: str):
        pass


    @abstractmethod
    def log_error(self, message: str):
        pass


    @abstractmethod
    def log_plain(self, message: str):
        pass




class BKFileLogger(IBKLogger):
    def __init__(self, logger_name: str, leave_stream_open: bool, log_root: str, file_max_size_bytes: float):
        self._file_io_helper: FileIOHelper = IOCContainer.get_instance_singleton(FileIOHelper) # type: ignore
        self._logger_name = logger_name
        self._leave_stream_open: bool = leave_stream_open
        self._log_root = log_root
        self._file_max_size_bytes = file_max_size_bytes
        self._logger_root = os.path.join(self._log_root, self._logger_name)
        self._last_day = datetime.datetime.now().strftime(bk_time.only_date_format)
        self._file_counter = 1
        if not os.path.isdir(self._logger_root):
            os.makedirs(self._logger_root)
        else:
            # initialize file counter
            file_names = os.listdir(self._logger_root)
            for file_name in file_names:
                if file_name.startswith(self._last_day):
                    if "_" in file_name:
                        file_num_start_index = file_name.find("_")
                        if file_num_start_index != -1:
                            file_num_start_index += 1
                            file_num = int(file_name[file_num_start_index:])
                            if file_num > self._file_counter:
                                self._file_counter = file_num


    def _log(self, full_message: str):
        now = datetime.datetime.now()
        today = now.strftime(bk_time.only_date_format)
        if today != self._last_day:
            self._file_counter = 1
        file_name = f"{today}_{self._file_counter}"
        message = now.strftime(bk_time.micro_format) + " : " + full_message
        file_path = os.path.join(self._logger_root, file_name)
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            if file_size > self._file_max_size_bytes:
                self._file_counter += 1
                file_name = f"{today}_{self._file_counter}"
                file_path = os.path.join(self._logger_root, file_name)
        self._file_io_helper.append_line(message, file_path, self._leave_stream_open)


    def log_info(self, message: str):
        self._log(f"[INFO]\t{message}")


    def log_debug(self, message: str):
        self._log(f"[DEBUG]\t{message}")


    def log_error(self, message: str):
        self._log(f"[ERROR]\t{message}")


    def log_plain(self, message: str):
        self._log(f"{message}")


    @property
    def leave_stream_open(self):
        return self._leave_stream_open




class BKConsoleLogger(IBKLogger):
    def __init__(self, logger_name):
        self._logger_name = logger_name
        self._console_io_helper: ConsoleIOHelper = IOCContainer.get_instance_singleton(ConsoleIOHelper) # type: ignore


    def _log(self, type: str, full_message: str):
        # msg = datetime.datetime.now().strftime(bk_time.micro_format) + "-[" + self._logger_name + "] : " + f"[{type}]\t" + full_message
        d = {'t': datetime.datetime.now().strftime(bk_time.micro_format), 'logger': self._logger_name, "type": type, "msg": full_message}
        msg = json_encoding.encode(d)
        self._console_io_helper.console_write(msg)


    def log_info(self, message: str):
        self._log("INFO", message)


    def log_debug(self, message: str):
        self._log("DEBUG", message)


    def log_error(self, message: str):
        self._log("ERROR", message)


    def log_plain(self, message: str):
        self._log("", message)


class BKDummyLogger(IBKLogger):


    def log_info(self, message: str):
        pass


    def log_debug(self, message: str):
        pass


    def log_error(self, message: str):
        pass


    def log_plain(self, message: str):
        pass




class BKHybridLogger(IBKLogger):
    def __init__(self, file_logger: IBKLogger, console_logger: IBKLogger):
        self.__file_logger = file_logger
        self.__console_logger = console_logger


    def log_info(self, message: str):
        self.__file_logger.log_info(message)
        self.__console_logger.log_info(message)


    def log_debug(self, message: str):
        self.__file_logger.log_debug(message)
        self.__console_logger.log_debug(message)


    def log_error(self, message: str):
        self.__file_logger.log_error(message)
        self.__console_logger.log_error(message)


    def log_plain(self, message: str):
        self.__file_logger.log_plain(message)
        self.__console_logger.log_plain(message)




class BKLogging:


    class __LoggerType:
        Console = 0
        File = 1
        Hybrid = 2


    def __init__(self, log_root: str, log_console: bool = True, max_total_log_size_mib: float = 1000, file_max_size_mib: float = 10, log_file: bool = os.environ.get("BILIRA_ENV") not in {"production", "staging"}):
        self._log_root = log_root
        self._log_console = log_console
        self._log_file = log_file
        self._max_total_file_log_size_bytes = max_total_log_size_mib * 1024 * 1024
        self._file_max_size_bytes = file_max_size_mib * 1024 * 1024
        self._console_loggers: Dict[str, IBKLogger] = dict()
        self._file_loggers: Dict[str, IBKLogger] = dict()
        self._hybrid_loggers: Dict[str, IBKLogger] = dict()
        self._hybrid_logger = self.get_logger_hybrid(f"{type(self).__name__}")
        thread_helper: BKThreadHelper = IOCContainer.get_instance_singleton(BKThreadHelper) # type: ignore
        thread_helper.thread_start(self._file_delete_thread, is_background=True, thread_name="bk_logging_file_delete_thread")


    # old file delete thread
    # def _file_delete_thread(self):
    #     while True:
    #         try:
    #             delta = datetime.timedelta(days=-self._delete_files_after_days)
    #             threshold_date = datetime.datetime.now() + delta
    #             threshold_date = threshold_date.strftime(bk_time.only_date_format)
    #             directories = os.listdir(self._log_root)
    #             for directory in directories:
    #                 if directory == ".DS_Store":
    #                     continue
    #                 directory_full_path = os.path.join(self._log_root, directory)
    #                 files = os.listdir(directory_full_path)
    #                 for file in files:
    #                     file_full_path = os.path.join(directory_full_path, file)
    #                     if file <= threshold_date:
    #                         os.remove(file_full_path)
    #                         self._hybrid_logger.log_info(f"Removed file {file_full_path}")
    #         except:
    #             self._hybrid_logger.log_error(f"Error trying to delete log files: {traceback.format_exc()}")
    #
    #         time.sleep(1800)


    def _file_delete_thread(self):
        while True:
            try:
                total_log_size = misc_helper.get_directory_size_bytes(self._log_root)


                def file_sort_key(filename):
                    if "_" not in filename:
                        return datetime.datetime.strptime(filename, "%Y%m%d")
                    else:
                        underscore_index = filename.index("_")
                        date_part = filename[:underscore_index]
                        d = datetime.datetime.strptime(date_part, "%Y%m%d")
                        counter = int(filename[underscore_index + 1:])
                        d = d + datetime.timedelta(seconds=counter)
                        return d


                if total_log_size > self._max_total_file_log_size_bytes:
                    remaining_delete_size = total_log_size - self._max_total_file_log_size_bytes
                    dir_sorted = sorted([o for o in os.walk("z_logs") if ".DS_Store" not in o[2]], key=lambda o: len(o[2]), reverse=True)
                    for dirpath, _, filenames in dir_sorted:
                        filenames.sort(key=file_sort_key)
                        if len(filenames) > 1 and remaining_delete_size > 0:
                            filepath = os.path.join(dirpath, filenames[0])
                            file_size = os.path.getsize(filepath)
                            os.remove(filepath)
                            remaining_delete_size -= file_size
                            if remaining_delete_size <= 0:
                                break
            except:
                self._hybrid_logger.log_error(f"Error trying to delete log files: {traceback.format_exc()}")


            time.sleep(5)


    def _get_logger(self, loggers: Dict[str, IBKLogger], logger_name: str, logger_type: int, leave_stream_open: bool = False) -> IBKLogger:
        logger = None
        if logger_name not in loggers:
            if logger_type == BKLogging.__LoggerType.File:
                if self._log_file:
                    logger = BKFileLogger(logger_name, leave_stream_open, self._log_root, self._file_max_size_bytes)
                else:
                    logger = BKDummyLogger()
            elif logger_type == BKLogging.__LoggerType.Console:
                if self._log_console:
                    logger = BKConsoleLogger(logger_name)
                else:
                    logger = BKDummyLogger()
            #elif logger_type == BKLogging.__LoggerType.Hybrid:
            else:
                console_logger = self.get_logger_console(logger_name)
                file_logger = self.get_logger_file(logger_name)
                logger = BKHybridLogger(file_logger, console_logger)
            loggers[logger_name] = logger
        return loggers[logger_name]


    def get_logger_console(self, logger_name: str) -> IBKLogger:
        return self._get_logger(self._console_loggers, logger_name, BKLogging.__LoggerType.Console)


    def get_logger_file(self, logger_name: str, leave_stream_open: bool = False) -> IBKLogger:
        return self._get_logger(self._file_loggers, logger_name, BKLogging.__LoggerType.File, leave_stream_open=leave_stream_open)


    def get_logger_hybrid(self, logger_name: str, leave_stream_open: bool = False) -> IBKLogger:
        return self._get_logger(self._hybrid_loggers, logger_name, BKLogging.__LoggerType.Hybrid, leave_stream_open=leave_stream_open)













