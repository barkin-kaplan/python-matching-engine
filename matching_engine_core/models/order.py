from dataclasses import dataclass, field
from decimal import Decimal

from helper import bk_decimal
from matching_engine_core.models.order_status import OrderStatus
from matching_engine_core.models.side import Side


@dataclass
class Order:
    cl_ord_id: str
    order_id: str
    side: Side
    qty: Decimal
    price: Decimal
    symbol: str
    status: OrderStatus = OrderStatus.PendingNew
    filled_qty: Decimal = Decimal("0")
    
    @property
    def open_qty(self) -> Decimal:
        return self.qty - self.filled_qty
        
    def update_state_after_transaction(self):
        if bk_decimal.is_epsilon_equal(self.filled_qty, self.qty):
            self.status = OrderStatus.Filled
        elif bk_decimal.epsilon_gt(self.filled_qty, Decimal("0")):
            self.status = OrderStatus.PartiallyFilled
        
        
    