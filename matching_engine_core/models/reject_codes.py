from enum import Enum


class RejectCode(Enum):
    OrderDoesNotExist = 0
    NewQtyCantBeLessThanOrEqualToFilledQty = 1
    PriceOrQtyMustBeChanged = 2
    