from enum import Enum


class RejectCode(Enum):
    OrderDoesNotExist = 0
    NewQtyCantBeLessThanOpenQty = 1