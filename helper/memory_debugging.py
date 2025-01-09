
import io
import json
import random
from threading import Thread
from time import sleep
import traceback
from typing import Dict, List
import objgraph

from helper.threading.bl_worker import BLWorker

# Objgraph kurulumu: https://objgraph.readthedocs.io/en/stable/
# dotx kurulumunda hata çıkarsa .dot formatındaki grafikleri şu şekilde GPT'ye çizdirebiliyoruz:
# https://chatgpt.com/c/3aac1c7f-3aee-4a30-9cea-88ecc15cc1f1

class MemoryDebugLogger(BLWorker):

    SLEEP_INTERVAL_SECONDS: float = 15
    TOP_N_TYPES: int = 5
    SUSPECTED_TYPES: List[str] = ["list", "dict", "tuple"]
    
    def __init__(self) -> None:
        super().__init__(thread_helper = True, logger_factory = True, default_loggers = True)
        self.data_logger = self.logger_factory.get_logger_file(f"{type(self).__name__}_data")
        
        self.__running = False
        
    def start(self):
        self.__running = True
        self.thread_helper.thread_start(self.memory_debugger_thread_main, True)
    
    def memory_debugger_thread_main(self):
        while self.__running:
            try:
                type_stats = dict(sorted(list(objgraph.typestats().items()), key=lambda x: x[1], reverse=True)[:self.TOP_N_TYPES])
                self.data_logger.log_info(json.dumps(type_stats, indent=4))
                
                type_graphs = {}
                for t in type_stats.keys():
                    if t in self.SUSPECTED_TYPES:
                        type_graphs[t] = self.get_graph_type_chains(t)
                self.data_logger.log_info(json.dumps(type_graphs, indent=4))

                sleep(self.SLEEP_INTERVAL_SECONDS)
            except Exception:
                self.exception_logger.log_error(f"Unexpected error on memory debugger thread:\n{traceback.format_exc()}")
                
    def get_graph_type_chains(self, type_name: str, first: bool = True, last: bool = True, rand_n: int = 3):
        strs = []
        if first:
            dot_output = io.StringIO()
            objgraph.show_chain(objgraph.find_backref_chain(objgraph.by_type(type_name)[0], objgraph.is_proper_module), output=dot_output)
            strs.append(dot_output.getvalue().replace("\n", ""))
        for n in range(rand_n):
            dot_output = io.StringIO()
            objgraph.show_chain(objgraph.find_backref_chain(random.choice(objgraph.by_type(type_name)), objgraph.is_proper_module), output=dot_output)
            strs.append(dot_output.getvalue().replace("\n", ""))
        if last:
            dot_output = io.StringIO()
            objgraph.show_chain(objgraph.find_backref_chain(objgraph.by_type(type_name)[-1], objgraph.is_proper_module), output=dot_output)
            strs.append(dot_output.getvalue().replace("\n", ""))
        return strs
                
    # # Saves the graph of 1 random object of type "type_name".
    # def graph_type_chain(self, type_name: str):
    #     objgraph.show_chain(
    #         objgraph.find_backref_chain(
    #         random.choice(objgraph.by_type(type_name)),
    #         objgraph.is_proper_module))

    # # Saves the graph of the first, last and "count" random objects of type "type_name". 
    # def graph_type_chains(self, type_name: str, count: int = 8):
    #     objgraph.show_chain(
    #         objgraph.find_backref_chain(
    #         random.choice(objgraph.by_type(type_name)),
    #         objgraph.is_proper_module))


# Periodically prints the number of instances of each type in memory.
def log_type_counts(period_seconds: float = 5):
    def __log_type_counts(period_seconds: float = 5):
        while True:
            print("---------- TYPE COUNTS ----------")
            objgraph.show_most_common_types()
            print("---------- ----------- ----------")
            sleep(period_seconds)
    Thread(target=__log_type_counts, args=(period_seconds,)).start()

# Waits "wait_seconds" seconds and graphs the type chain of a random instance of "type_name".
def graph_type_chain(type_name: str, wait_seconds: float = 30):
    def __graph_type_chain(wait_seconds: float = 30):
        sleep(wait_seconds)
        objgraph.show_chain(
            objgraph.find_backref_chain(
            random.choice(objgraph.by_type(type_name)),
            objgraph.is_proper_module))
    Thread(target=__graph_type_chain, args=(wait_seconds,)).start()