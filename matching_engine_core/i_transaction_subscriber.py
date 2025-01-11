from abc import ABC, abstractmethod

from matching_engine_core.models.order import Order
from matching_engine_core.models.reject_codes import RejectCode
from matching_engine_core.models.trade import Trade


class ITransactionSubscriber(ABC):
    @abstractmethod
    def on_trade(self, trade: Trade):
        pass
    
    @abstractmethod
    def on_order_update(self, order: Order):
        pass
    
    @abstractmethod
    def on_cancel_reject(self, order: Order, reject_code: RejectCode):
        pass
    
    @abstractmethod
    def on_replace_reject(self, order: Order, reject_code: RejectCode):
        pass