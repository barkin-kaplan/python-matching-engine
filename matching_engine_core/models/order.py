from dataclasses import dataclass, field
from decimal import Decimal

from matching_engine_core.models.side import Side


@dataclass
class Order:
    cl_ord_id: str
    order_id: str
    side: Side
    qty: Decimal
    price: Decimal
    open_qty: Decimal = field(init=False)
    symbol: str
    
    def __post_init__(self):
        self.open_qty = self.qty
        
    