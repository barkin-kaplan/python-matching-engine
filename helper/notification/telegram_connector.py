import datetime
import queue
import time
import traceback

from typing import Callable, Dict
import requests

from helper.ioc_container import IOCContainer
from urllib.parse import quote

from helper.logging.bk_logging import BKLogging, IBKLogger
from helper.serialization.json import json_encoding
from helper.threading.bk_thread_helper import BKThreadHelper


class TelegramConnector:
    def __init__(self, token: str, chat_id: str):
        self.chat_id = chat_id
        self.bot_token = token
        self._endpoints: Dict[str, list[Callable[[TelegramConnector, str], None]]] = {}
        logger_factory: BKLogging = IOCContainer.get_instance_singleton(BKLogging)
        self._hybrid_logger: IBKLogger = logger_factory.get_logger_hybrid(f"{type(self).__name__}")
        self._message_queue: queue.Queue = queue.Queue()
        self._exception_wait_seconds = 5
        self._thread_helper: BKThreadHelper = IOCContainer.get_instance_singleton(BKThreadHelper)
        self._thread_helper.thread_start(self._send_message_thread, is_background=True, thread_name="telegram_send_message")
        self.offset = None

    def _send_message_thread(self):
        while True:
            try:
                message, is_critical = self._message_queue.get()
                self._send_message_with_retry(message, is_critical)
            except:
                self._hybrid_logger.log_error(f"Error in send message thread {traceback.format_exc()}")

    def _send_message_with_retry(self, message_raw: str, is_critical=True):
        try:
            message_formatted = self._validate_and_convert_message(message_raw)
            url = f'https://api.telegram.org/bot{self.bot_token}/sendMessage?chat_id={self.chat_id}&parse_mode=Markdown&text={message_formatted}'
            response = requests.get(url)
            self._hybrid_logger.log_info(url)
            if response.status_code != 200:
                self._hybrid_logger.log_error(f"{response.status_code} - {response.text}")
                if response.status_code == 429:
                    try:
                        response_dic = json_encoding.decode(response.text)
                        if "parameters" in response_dic:
                            retry_after = int(response_dic["parameters"].get("retry_after"))
                            self._hybrid_logger.log_info(f"Response 429 received waiting for {retry_after}")
                            time.sleep(retry_after)
                            self._send_message_with_retry(message_raw, is_critical)
                            return
                    except:
                        self._hybrid_logger.log_error(f"Error parsing response dic for 429 response {traceback.format_exc()}")
        except:
            self._hybrid_logger.log_error(f"Couldn't send message. Retrying in {self._exception_wait_seconds}. {traceback.format_exc()}")
            time.sleep(self._exception_wait_seconds)
            self._send_message_with_retry(message_raw, is_critical)

    @staticmethod
    def _validate_and_convert_message(message):
        message = message.replace("_", "\\_").replace("*", "\\*").replace("[", "\\[").replace("`", "\\`")
        message = quote(message)
        return message

    def send_message_with_retry(self, message: str, is_critical=True):
        self.send_message(message, is_critical)

    def send_message(self, message: str, is_critical=True):
        message = f"[{datetime.datetime.now().isoformat()}]\n{message}"
        self._message_queue.put((message, is_critical))

    def _general_command_handle(self, update):
        try:
            chat_id = str(update["message"]["chat"]["id"])
            if chat_id != self.chat_id:
                return
            message = update["message"]["text"]
            message_splitted = message.split(" ")
            message_endpoint = message_splitted[0][1:]
            if message_endpoint in self._endpoints:
                for func in self._endpoints[message_endpoint]:
                    func(self, message)
        except:
            self._hybrid_logger.log_error(f"Error handling incoming message {update}, {traceback.format_exc()}")

    def add_handler(self, handle_name: str, handle_func: Callable[['TelegramConnector', str], None]):
        if handle_name not in self._endpoints:
            self._endpoints[handle_name] = []
        self._endpoints[handle_name].append(handle_func)
        
    def _get_updates(self):
        url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
        params = {'timeout': 100, 'offset': self.offset}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data['ok']:
                return data['result']
        return []

    def _poll_thread(self):
        while True:
            updates = self._get_updates()
            for update in updates:
                if 'message' in update:
                    self._general_command_handle(update)                
                # Update the offset to avoid processing the same updates again
                self.offset = update['update_id'] + 1
            time.sleep(1)
                
    def start(self):
        self._thread_helper.thread_start(self._poll_thread, is_background=True, thread_name="telegram_connector_poll_thread")
