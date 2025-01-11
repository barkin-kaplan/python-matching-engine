from enum import Enum


class OrderStatus(Enum):
    PendingNew = 0
    Open = 1
    Canceled = 2
    Rejected = 3
    PartiallyFilled = 4
    Filled = 5