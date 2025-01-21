from dataclasses import dataclass, field
from decimal import Decimal

from helper import bk_decimal, bk_time
from matching_engine_core.models.order_status import OrderStatus
from matching_engine_core.models.side import Side

_OPEN_STATES = {OrderStatus.PendingNew, OrderStatus.Open, OrderStatus.PartiallyFilled}

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
    timestamp: int = field(default_factory=bk_time.get_current_time_millis)
    
    @property
    def open_qty(self) -> Decimal:
        return self.qty - self.filled_qty
    
    @property
    def is_open(self) -> bool:
        return self.status in _OPEN_STATES
        
    def update_state_after_transaction(self):
        if bk_decimal.epsilon_equal(self.filled_qty, self.qty):
            self.status = OrderStatus.Filled
        elif bk_decimal.epsilon_gt(self.filled_qty, Decimal("0")):
            self.status = OrderStatus.PartiallyFilled
        
        
    