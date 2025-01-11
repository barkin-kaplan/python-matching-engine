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
        
def submit_order(ob: Orderbook, price: Decimal, qty: Decimal, side: Side):
    order = Order(cl_ord_id=string_helper.generate_uuid(),
                  order_id=string_helper.generate_uuid(),
                  side=side,
                  qty=qty,
                  price=price,
                  symbol="test")
    ob.submit_order(order)
    return order
    
def assert_orders_length(ob: Orderbook, buy_orders_length: int, sell_orders_length: int):
    buy_orders = list(ob.in_order_buy_orders())
    sell_orders = list(ob.in_order_sell_orders())
    assert len(buy_orders) == buy_orders_length
    assert len(sell_orders) == sell_orders_length

def test_single_order_entry_buy():
    ob = Orderbook("test")
    submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000003"), side=Side.Buy)
    assert_orders_length(ob, 1, 0)
    buy_orders = list(ob.in_order_buy_orders())
    assert buy_orders[0].qty == Decimal("0.000000003")
    assert ob._best_bid == Decimal("0.000000005")
    
def test_single_order_entry_sell():
    ob = Orderbook("test")
    submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000003"), side=Side.Sell)
    assert_orders_length(ob, 0, 1)
    buy_orders = list(ob.in_order_sell_orders())
    assert buy_orders[0].qty == Decimal("0.000000003")
    assert ob._best_ask == Decimal("0.000000005")
    
def test_match_single_order():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    buy_order = submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000003"), side=Side.Buy)
    assert ob._best_bid == buy_order.price
    sell_order = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000003"), side=Side.Sell)
    assert len(subscriber.trades) == 1
    assert subscriber.trades[0].qty == Decimal("0.000000003")
    assert subscriber.trades[0].price == Decimal("0.000000005")
    assert subscriber.trades[0].buy_order_id == buy_order.order_id
    assert subscriber.trades[0].sell_order_id == sell_order.order_id
    assert ob._best_bid is None
    assert ob._best_ask is None
    assert_orders_length(ob, 0, 0)
    
def test_single_partial_fill():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000006"), side=Side.Buy)
    assert_orders_length(ob, 1, 0)
    submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000003"), side=Side.Sell)
    assert_orders_length(ob, 1, 0)
    buy_orders = list(ob.in_order_buy_orders())
    assert buy_orders[0].open_qty == Decimal("0.000000003")
    assert len(subscriber.trades) == 1
    assert subscriber.trades[0].qty == Decimal("0.000000003")
    
def test_place_two_level_both_sides():
    ob = Orderbook("test")
    submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000006"), side=Side.Buy)
    submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000007"), side=Side.Buy)
    assert_orders_length(ob, 2, 0)
    buy_orders = list(ob.in_order_buy_orders())
    assert buy_orders[0].price == Decimal("0.000000005")
    assert buy_orders[1].price == Decimal("0.000000004")
    assert buy_orders[0].open_qty == Decimal("0.000000006")
    assert buy_orders[1].open_qty == Decimal("0.000000007")
    submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000011"), side=Side.Sell)
    submit_order(ob, price=Decimal("0.000000007"), qty=Decimal("0.000000013"), side=Side.Sell)
    assert_orders_length(ob, 2, 2)
    sell_orders = list(ob.in_order_sell_orders())
    assert sell_orders[0].price == Decimal("0.000000006")
    assert sell_orders[1].price == Decimal("0.000000007")
    assert sell_orders[0].qty == Decimal("0.000000011")
    assert sell_orders[1].qty == Decimal("0.000000013")
    
def test_place_buy_match_with_two_sell_orders_at_same_level():
    ob = Orderbook("test")    
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    so1 = submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000011"), side=Side.Sell)
    so2 = submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000013"), side=Side.Sell)
    bo1 = submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000014"), side=Side.Buy)
    assert len(subscriber.trades) == 2
    assert subscriber.trades[0].qty == so1.qty
    assert subscriber.trades[0].price == Decimal("0.000000006")
    assert subscriber.trades[0].buy_order_id == bo1.order_id
    assert subscriber.trades[0].sell_order_id == so1.order_id
    assert subscriber.trades[1].qty == Decimal("0.000000003")
    assert subscriber.trades[1].price == Decimal("0.000000006")
    assert subscriber.trades[1].buy_order_id == bo1.order_id
    assert subscriber.trades[1].sell_order_id == so2.order_id
    
def test_place_sell_match_with_two_buy_orders_at_same_level():
    ob = Orderbook("test")    
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    ob.subscribe(subscriber)
    bo1 = submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000011"), side=Side.Buy)
    bo2 = submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000013"), side=Side.Buy)
    so1 = submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000014"), side=Side.Sell)
    assert len(subscriber.trades) == 2
    assert subscriber.trades[0].qty == bo1.qty
    assert subscriber.trades[0].price == Decimal("0.000000006")
    assert subscriber.trades[0].sell_order_id == so1.order_id
    assert subscriber.trades[0].buy_order_id == bo1.order_id
    assert subscriber.trades[1].qty == Decimal("0.000000003")
    assert subscriber.trades[1].price == Decimal("0.000000006")
    assert subscriber.trades[1].sell_order_id == so1.order_id
    assert subscriber.trades[1].buy_order_id == bo2.order_id
    

def test_place_buy_match_with_two_sell_orders_at_different_levels_leaving_open_at_third_level():
    ob = Orderbook("test")    
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    so1 = submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000011"), side=Side.Sell)
    so2 = submit_order(ob, price=Decimal("0.000000007"), qty=Decimal("0.000000013"), side=Side.Sell)
    so3 = submit_order(ob, price=Decimal("0.000000008"), qty=Decimal("0.000000013"), side=Side.Sell)
    bo1 = submit_order(ob, price=Decimal("0.000000007"), qty=Decimal("0.000000024"), side=Side.Buy)
    assert len(subscriber.trades) == 2
    assert subscriber.trades[0].qty == so1.qty
    assert so1.open_qty == Decimal("0")
    assert subscriber.trades[0].price == so1.price
    assert subscriber.trades[0].buy_order_id == bo1.order_id
    assert subscriber.trades[0].sell_order_id == so1.order_id
    assert subscriber.trades[1].qty == so2.qty
    assert so2.open_qty == Decimal("0")
    assert subscriber.trades[1].price == so2.price
    assert subscriber.trades[1].buy_order_id == bo1.order_id
    assert subscriber.trades[1].sell_order_id == so2.order_id
    assert_orders_length(ob, 0, 1)
    sell_orders = list(ob.in_order_sell_orders())
    assert sell_orders[0] == so3
    assert so3.open_qty == so3.qty
    
    
def test_place_sell_match_with_two_buy_orders_at_different_levels_leaving_open_at_third_level():
    ob = Orderbook("test")    
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    ob.subscribe(subscriber)
    bo1 = submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000011"), side=Side.Buy)
    bo2 = submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000013"), side=Side.Buy)
    bo3 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000013"), side=Side.Buy)
    so1 = submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000024"), side=Side.Sell)
    assert len(subscriber.trades) == 2
    assert subscriber.trades[0].qty == bo1.qty
    assert bo1.open_qty == Decimal("0")
    assert subscriber.trades[0].price == bo1.price
    assert subscriber.trades[0].sell_order_id == so1.order_id
    assert subscriber.trades[0].buy_order_id == bo1.order_id
    assert subscriber.trades[1].qty == bo2.qty
    assert bo1.open_qty == Decimal("0")
    assert subscriber.trades[1].price == bo2.price
    assert subscriber.trades[1].sell_order_id == so1.order_id
    assert subscriber.trades[1].buy_order_id == bo2.order_id
    assert_orders_length(ob, 1, 0)
    buy_orders = list(ob.in_order_buy_orders())
    assert buy_orders[0] == bo3
    assert bo3.open_qty == bo3.qty
    
def test_place_buy_sweep_multiple_orders_at_multiple_levels():
    ob = Orderbook("test")    
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    ob.subscribe(subscriber)
    so1 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000011"), side=Side.Sell)
    so2 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000014"), side=Side.Sell)
    so3 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000018"), side=Side.Sell)
    so4 = submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000013"), side=Side.Sell)
    so5 = submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000010"), side=Side.Sell)
    so6 = submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000013"), side=Side.Sell)
    bo1 = submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000200"), side=Side.Buy)
    sell_orders = [so1, so2, so3, so4, so5, so6]
    def assert_filled_case(b: Order, trade_index: int):
        # just a helper function
        assert subscriber.trades[trade_index].qty == b.qty
        assert so1.open_qty == Decimal("0")
        assert subscriber.trades[trade_index].price == b.price
        assert subscriber.trades[trade_index].buy_order_id == bo1.order_id
        assert subscriber.trades[trade_index].sell_order_id == b.order_id
    assert len(subscriber.trades) == 6
    for i in range(len(sell_orders)):
        assert_filled_case(sell_orders[i], i)
    assert_orders_length(ob, 1, 0)
    assert bo1.open_qty == bo1.qty - sum([o.qty for o in sell_orders])
    assert ob._best_bid == bo1.price
    assert ob._best_ask is None
    
    
def test_place_sell_sweep_multiple_orders_at_multiple_levels():
    ob = Orderbook("test")    
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    ob.subscribe(subscriber)
    bo1 = submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000011"), side=Side.Buy)
    bo2 = submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000014"), side=Side.Buy)
    bo3 = submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000018"), side=Side.Buy)
    bo4 = submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000013"), side=Side.Buy)
    bo5 = submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000010"), side=Side.Buy)
    bo6 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000013"), side=Side.Buy)
    so1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000200"), side=Side.Sell)
    buy_orders = [bo1, bo2, bo3, bo4, bo5, bo6]
    def assert_filled_case(b: Order, trade_index: int):
        # just a helper function
        assert subscriber.trades[trade_index].qty == b.qty
        assert bo1.open_qty == Decimal("0")
        assert subscriber.trades[trade_index].price == b.price
        assert subscriber.trades[trade_index].sell_order_id == so1.order_id
        assert subscriber.trades[trade_index].buy_order_id == b.order_id
    assert len(subscriber.trades) == 6
    for i in range(len(buy_orders)):
        assert_filled_case(buy_orders[i], i)
    assert_orders_length(ob, 0, 1)
    assert so1.open_qty == so1.qty - sum([o.qty for o in buy_orders])
    assert ob._best_bid is None
    assert ob._best_ask == so1.price
    
def test_constant_best_ask():
    ob = Orderbook("test")
    o = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Sell)
    assert ob._best_bid is None
    assert ob._best_ask == o.price
    submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000004"), side=Side.Sell)
    assert ob._best_ask == o.price
    submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000004"), side=Side.Sell)
    assert ob._best_ask == o.price
    assert ob._best_bid is None
    
def test_constant_best_bid():
    ob = Orderbook("test")
    o = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob._best_ask is None
    assert ob._best_bid == o.price
    submit_order(ob, price=Decimal("0.000000002"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob._best_bid == o.price
    submit_order(ob, price=Decimal("0.000000002"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob._best_bid == o.price
    assert ob._best_ask is None
    
def test_decreasing_best_ask():
    ob = Orderbook("test")
    o1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Sell)
    assert ob._best_bid is None
    assert ob._best_ask == o1.price
    o2 = submit_order(ob, price=Decimal("0.000000002"), qty=Decimal("0.000000004"), side=Side.Sell)
    assert ob._best_ask == o2.price
    o3 = submit_order(ob, price=Decimal("0.000000002"), qty=Decimal("0.000000004"), side=Side.Sell)
    assert ob._best_ask == o2.price
    
def test_decreasing_best_bid():
    ob = Orderbook("test")
    o1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob._best_ask is None
    assert ob._best_bid == o1.price
    o2 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob._best_bid == o2.price
    o3 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob._best_bid == o2.price