from decimal import Decimal
from typing import List
from unittest.mock import MagicMock
from helper import string_helper
from matching_engine_core.i_transaction_subscriber import ITransactionSubscriber
from matching_engine_core.models.order import Order
from matching_engine_core.models.side import Side
from matching_engine_core.models.trade import Trade
from matching_engine_core.orderbook import Orderbook

class MockTransSubscriber(ITransactionSubscriber):
    def __init__(self):
        super().__init__()
        self.trades: List[Trade] = []
        
    def on_trade(self, trade: Trade):
        self.trades.append(trade)

def test_ob_ops():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    buy_order = Order(cl_ord_id=string_helper.generate_uuid(),
                  order_id=string_helper.generate_uuid(),
                  side=Side.Buy,
                  qty=Decimal("0.000000003"),
                  price=Decimal("0.000000005"),
                  symbol="test",
                  open_qty=Decimal("0.000000003"))
    ob.submit_order(buy_order)
    buy_orders = list(ob.in_order_buy_orders())
    sell_orders = list(ob.in_order_sell_orders())
    assert len(buy_orders) == 1
    assert len(sell_orders) == 0
    assert buy_orders[0].qty == Decimal("0.000000003")
    
    sell_order = Order(cl_ord_id=string_helper.generate_uuid(),
                  order_id=string_helper.generate_uuid(),
                  side=Side.Sell,
                  qty=Decimal("0.000000003"),
                  price=Decimal("0.000000005"),
                  symbol="test",
                  open_qty=Decimal("0.000000003"))
    ob.submit_order(sell_order)
    assert len(subscriber.trades) == 1
    assert subscriber.trades[0].qty == Decimal("0.000000003")
    buy_orders = list(ob.in_order_buy_orders())
    sell_orders = list(ob.in_order_sell_orders())
    assert len(buy_orders) == 0
    assert len(sell_orders) == 0
