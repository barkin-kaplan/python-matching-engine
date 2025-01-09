from dataclasses import dataclass
from decimal import Decimal

from matching_engine_core.models.side import Side


@dataclass
class Order:
    cl_ord_id: str
    order_id: str
    side: Side
    qty: Decimal
    price: Decimal
    open_qty: Decimal
    symbol: str
    