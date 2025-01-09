from abc import ABC, abstractmethod

from matching_engine_core.models.trade import Trade


class ITransactionSubscriber(ABC):
    @abstractmethod
    def on_trade(self, trade: Trade):
        pass
    