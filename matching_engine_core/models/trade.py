from dataclasses import dataclass, field
from decimal import Decimal
from helper import string_helper
from matching_engine_core.models.side import Side

@dataclass
class Trade:
    active_side: Side
    buy_order_id: str
    sell_order_id: str
    qty: Decimal
    price: Decimal
    trade_id: str = field(default_factory=lambda: string_helper.generate_uuid())

    def __eq__(self, other):
        if not isinstance(other, Trade):
            return False
        return (
            self.active_side == other.active_side and
            self.buy_order_id == other.buy_order_id and
            self.sell_order_id == other.sell_order_id and
            self.qty == other.qty and
            self.price == other.price and
            self.trade_id == other.trade_id
        )
