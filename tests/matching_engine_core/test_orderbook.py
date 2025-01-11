import copy
from decimal import Decimal
from typing import Dict, List, Tuple
from unittest.mock import MagicMock
from helper import string_helper
from matching_engine_core.i_transaction_subscriber import ITransactionSubscriber
from matching_engine_core.models.order import Order
from matching_engine_core.models.order_status import OrderStatus
from matching_engine_core.models.reject_codes import RejectCode
from matching_engine_core.models.side import Side
from matching_engine_core.models.trade import Trade
from matching_engine_core.orderbook import Orderbook

class MockTransSubscriber(ITransactionSubscriber):
    def __init__(self):
        super().__init__()
        self.trades: List[Trade] = []
        self.order_updates: Dict[str, List[Order]] = dict()
        self.cancel_rejects: Dict[str, List[Tuple[Order, RejectCode]]] = dict()
        self.replace_rejects: Dict[str, List[Tuple[Order, RejectCode]]] = dict()
        
    def on_trade(self, trade: Trade):
        self.trades.append(trade)
        
    def on_order_update(self, order: Order):
        order_copy = copy.deepcopy(order)
        related_order_updates = self.order_updates.get(order_copy.order_id)
        if related_order_updates is None:
            related_order_updates = list()
            self.order_updates[order_copy.order_id] = related_order_updates
        related_order_updates.append(order_copy)
        
    def on_cancel_reject(self, order: Order, reject_code: RejectCode):
        order_copy = copy.deepcopy(order)
        related_rejects = self.cancel_rejects.get(order_copy.order_id)
        if related_rejects is None:
            related_rejects = list()
            self.cancel_rejects[order_copy.order_id] = related_rejects
        related_rejects.append((order_copy, reject_code))
    
    def on_replace_reject(self, order: Order, reject_code: RejectCode):
        order_copy = copy.deepcopy(order)
        related_rejects = self.replace_rejects.get(order_copy.order_id)
        if related_rejects is None:
            related_rejects = list()
            self.replace_rejects[order_copy.order_id] = related_rejects
        related_rejects.append((order_copy, reject_code))
    
def create_order(price: Decimal, qty: Decimal, side: Side):
    order = Order(cl_ord_id=string_helper.generate_uuid(),
                  order_id=string_helper.generate_uuid(),
                  side=side,
                  qty=qty,
                  price=price,
                  symbol="test")
    
    return order

        
def submit_order(ob: Orderbook, price: Decimal, qty: Decimal, side: Side):
    order = create_order(price, qty, side)
    ob.submit_order(order)
    return order
    
def assert_orders_length(ob: Orderbook, buy_orders_length: int, sell_orders_length: int):
    buy_orders = list(ob.in_order_buy_orders())
    sell_orders = list(ob.in_order_sell_orders())
    assert len(buy_orders) == buy_orders_length
    assert len(sell_orders) == sell_orders_length

def test_single_order_entry_buy():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    order = submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000003"), side=Side.Buy)
    assert_orders_length(ob, 1, 0)
    buy_orders = list(ob.in_order_buy_orders())
    assert buy_orders[0].qty == Decimal("0.000000003")
    assert ob._best_bid == Decimal("0.000000005")
    assert buy_orders[0].status == OrderStatus.Open
    o_updates = subscriber.order_updates.get(order.order_id)
    assert o_updates is not None
    assert len(o_updates) == 1
    assert o_updates[0].status == OrderStatus.Open
    
def test_single_order_entry_sell():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    order = submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000003"), side=Side.Sell)
    assert_orders_length(ob, 0, 1)
    buy_orders = list(ob.in_order_sell_orders())
    assert buy_orders[0].qty == Decimal("0.000000003")
    assert ob._best_ask == Decimal("0.000000005")
    assert buy_orders[0].status == OrderStatus.Open
    o_updates = subscriber.order_updates.get(order.order_id)
    assert o_updates is not None
    assert len(o_updates) == 1
    assert o_updates[0].status == OrderStatus.Open
    
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
    assert buy_order.status == OrderStatus.Filled
    assert sell_order.status == OrderStatus.Filled
    assert subscriber.order_updates[buy_order.order_id][0].status == OrderStatus.Open
    assert subscriber.order_updates[buy_order.order_id][1].status == OrderStatus.Filled
    assert subscriber.order_updates[sell_order.order_id][0].status == OrderStatus.Open
    assert subscriber.order_updates[sell_order.order_id][1].status == OrderStatus.Filled
    
def test_single_partial_fill():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    bo = submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000006"), side=Side.Buy)
    assert_orders_length(ob, 1, 0)
    so = submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000003"), side=Side.Sell)
    assert_orders_length(ob, 1, 0)
    buy_orders = list(ob.in_order_buy_orders())
    assert buy_orders[0].open_qty == Decimal("0.000000003")
    assert len(subscriber.trades) == 1
    assert subscriber.trades[0].qty == Decimal("0.000000003")
    assert bo.status == OrderStatus.PartiallyFilled
    assert so.status == OrderStatus.Filled
    assert len(subscriber.order_updates) == 2
    assert len(subscriber.order_updates[bo.order_id]) == 2
    assert len(subscriber.order_updates[so.order_id]) == 2
    assert subscriber.order_updates[bo.order_id][0].status == OrderStatus.Open
    assert subscriber.order_updates[bo.order_id][1].status == OrderStatus.PartiallyFilled
    assert subscriber.order_updates[so.order_id][0].status == OrderStatus.Open
    assert subscriber.order_updates[so.order_id][1].status == OrderStatus.Filled
    
    
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
    assert buy_orders[0].status == buy_orders[1].status == OrderStatus.Open
    assert buy_orders[1].status == buy_orders[1].status == OrderStatus.Open
    submit_order(ob, price=Decimal("0.000000006"), qty=Decimal("0.000000011"), side=Side.Sell)
    submit_order(ob, price=Decimal("0.000000007"), qty=Decimal("0.000000013"), side=Side.Sell)
    assert_orders_length(ob, 2, 2)
    sell_orders = list(ob.in_order_sell_orders())
    assert sell_orders[0].price == Decimal("0.000000006")
    assert sell_orders[1].price == Decimal("0.000000007")
    assert sell_orders[0].qty == Decimal("0.000000011")
    assert sell_orders[1].qty == Decimal("0.000000013")
    assert sell_orders[0].status == sell_orders[1].status == OrderStatus.Open
    assert sell_orders[1].status == sell_orders[1].status == OrderStatus.Open
    
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
    assert so1.status == OrderStatus.Filled
    assert so2.status == OrderStatus.PartiallyFilled
    assert bo1.status == OrderStatus.Filled
    
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
    assert bo1.status == OrderStatus.Filled
    assert bo2.status == OrderStatus.PartiallyFilled
    assert so1.status == OrderStatus.Filled    

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
    assert so1.status == OrderStatus.Filled
    assert so2.status == OrderStatus.Filled
    assert so3.status == OrderStatus.Open
    assert bo1.status == OrderStatus.Filled
        
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
    assert bo1.status == OrderStatus.Filled
    assert bo2.status == OrderStatus.Filled
    assert bo3.status == OrderStatus.Open
    assert so1.status == OrderStatus.Filled
    
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
    assert all([o.status == OrderStatus.Filled for o in sell_orders])
    assert bo1.status == OrderStatus.PartiallyFilled
    
    
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
    assert all([o.status == OrderStatus.Filled for o in buy_orders])
    assert so1.status == OrderStatus.PartiallyFilled
    
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
    
def test_increasing_best_bid():
    ob = Orderbook("test")
    o1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob._best_ask is None
    assert ob._best_bid == o1.price
    o2 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob._best_bid == o2.price
    o3 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob._best_bid == o2.price
    
def test_single_cancel():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    o1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    ob.cancel_order(o1)
    assert len(subscriber.order_updates[o1.order_id]) == 2
    assert subscriber.order_updates[o1.order_id][1].status == OrderStatus.Canceled
    assert o1.status == OrderStatus.Canceled
    assert len(list(ob.in_order_buy_orders())) == 0
    assert len(list(ob.in_order_sell_orders())) == 0
    
def test_partial_filled_cancel():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    bo = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    so = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000002"), side=Side.Sell)
    ob.cancel_order(bo)
    assert len(subscriber.order_updates[bo.order_id]) == 3
    assert subscriber.order_updates[bo.order_id][0].status == OrderStatus.Open
    assert subscriber.order_updates[bo.order_id][1].status == OrderStatus.PartiallyFilled
    assert subscriber.order_updates[bo.order_id][2].status == OrderStatus.Canceled
    assert bo.status == OrderStatus.Canceled
    assert len(list(ob.in_order_buy_orders())) == 0
    assert len(list(ob.in_order_sell_orders())) == 0
    
def test_non_existing_cancel():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    bo = create_order(price=Decimal("0.000000009"), qty=Decimal("0.000000007"), side=Side.Buy)
    ob.cancel_order(bo)
    order_updates = subscriber.order_updates.get(bo.order_id)
    assert order_updates is None
    assert len(subscriber.cancel_rejects) == 1
    assert len(subscriber.cancel_rejects[bo.order_id]) == 1
    assert subscriber.cancel_rejects[bo.order_id][0][1] == RejectCode.OrderDoesNotExist
    
def test_non_existing_cancel_on_existing_level():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    bo1 = submit_order(ob, price=Decimal("0.000000009"), qty=Decimal("0.000000004"), side=Side.Buy)
    bo = create_order(price=Decimal("0.000000009"), qty=Decimal("0.000000007"), side=Side.Buy)
    ob.cancel_order(bo)
    order_updates = subscriber.order_updates.get(bo.order_id)
    assert order_updates is None
    assert len(subscriber.cancel_rejects) == 1
    assert len(subscriber.cancel_rejects[bo.order_id]) == 1
    assert subscriber.cancel_rejects[bo.order_id][0][1] == RejectCode.OrderDoesNotExist
    
def test_cancel_priority():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    bo1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    bo2 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000004"), side=Side.Buy)
    bo3 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000004"), side=Side.Buy)
    ob.cancel_order(bo2)
    buy_orders = list(ob.in_order_buy_orders())
    assert buy_orders[0] is bo3
    assert buy_orders[1] is bo1
    ob.cancel_order(bo3)
    buy_orders = list(ob.in_order_buy_orders())
    assert len(buy_orders) == 1
    assert buy_orders[0] is bo1
    assert len(subscriber.cancel_rejects) == 0
    assert len(subscriber.replace_rejects) == 0
    
