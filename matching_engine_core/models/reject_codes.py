from enum import Enum


class RejectCode(Enum):
    OrderDoesNotExist = 0
    NewQtyCantBeLessThanFilledQty = 1
    PriceOrQtyMustBeChanged = 2