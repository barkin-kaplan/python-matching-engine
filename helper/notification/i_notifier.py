from abc import ABC, abstractmethod


class INotifier(ABC):
    @abstractmethod
    def notify(self, message: str):
        pass
    