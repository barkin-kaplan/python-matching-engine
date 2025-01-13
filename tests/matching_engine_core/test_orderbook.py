import copy
from dataclasses import dataclass
from decimal import Decimal
import random
from typing import Dict, List, Optional, Set, Tuple
from unittest.mock import MagicMock
from helper import bk_decimal, string_helper
from matching_engine_core.i_transaction_subscriber import ITransactionSubscriber
from matching_engine_core.models.order import Order
from matching_engine_core.models.order_status import OrderStatus
from matching_engine_core.models.reject_codes import RejectCode
from matching_engine_core.models.side import Side
from matching_engine_core.models.trade import Trade
from matching_engine_core.orderbook import Orderbook

@dataclass
class ExpectedOrderUpdate:
    cl_ord_id: str
    order_id: str
    qty: Decimal
    price: Decimal
    status: OrderStatus
    filled_qty: Decimal
    
    def __hash__(self):
        return hash((self.cl_ord_id, self.order_id, self.qty, self.price, self.status, self.filled_qty))

    def __eq__(self, other):
        if not isinstance(other, ExpectedOrderUpdate):
            return NotImplemented
        return (self.cl_ord_id == other.cl_ord_id and
                self.order_id == other.order_id and
                self.qty == other.qty and
                self.price == other.price and
                self.status == other.status and
                self.filled_qty == other.filled_qty)

@dataclass
class RejectModel:
    order: Order
    reject_code: RejectCode

class MockTransSubscriber(ITransactionSubscriber):
    def __init__(self):
        super().__init__()
        self.trades: List[Trade] = []
        self.order_updates: Dict[str, List[Order]] = dict()
        self.cancel_rejects: Dict[str, List[RejectModel]] = dict()
        self.replace_rejects: Dict[str, List[RejectModel]] = dict()
        
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
        related_rejects.append(RejectModel(order_copy, reject_code))
    
    def on_replace_reject(self, order: Order, reject_code: RejectCode):
        order_copy = copy.deepcopy(order)
        related_rejects = self.replace_rejects.get(order_copy.order_id)
        if related_rejects is None:
            related_rejects = list()
            self.replace_rejects[order_copy.order_id] = related_rejects
        related_rejects.append(RejectModel(order_copy, reject_code))
    
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
    assert ob.best_bid == Decimal("0.000000005")
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
    assert ob.best_ask == Decimal("0.000000005")
    assert buy_orders[0].status == OrderStatus.Open
    o_updates = subscriber.order_updates.get(order.order_id)
    assert o_updates is not None
    assert len(o_updates) == 1
    assert o_updates[0].status == OrderStatus.Open
    
def test_multiple_order_entry_order():
    ob = Orderbook("test")
    submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000004"), side=Side.Buy)
    submit_order(ob, price=Decimal("0.000000002"), qty=Decimal("0.000000004"), side=Side.Buy)
    submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000004"), side=Side.Buy)
    buy_orders = list(ob.in_order_buy_orders())
    assert buy_orders[0].price == Decimal("0.000000005")
    assert buy_orders[1].price == Decimal("0.000000004")
    assert buy_orders[2].price == Decimal("0.000000003")
    assert buy_orders[3].price == Decimal("0.000000002")
    
def test_match_single_order():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    buy_order = submit_order(ob, price=Decimal("0.000000005"), qty=Decimal("0.000000003"), side=Side.Buy)
    assert ob.best_bid == buy_order.price
    sell_order = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000003"), side=Side.Sell)
    assert len(subscriber.trades) == 1
    assert subscriber.trades[0].qty == Decimal("0.000000003")
    assert subscriber.trades[0].price == Decimal("0.000000005")
    assert subscriber.trades[0].buy_order_id == buy_order.order_id
    assert subscriber.trades[0].sell_order_id == sell_order.order_id
    assert ob.best_bid is None
    assert ob.best_ask is None
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
    assert ob.best_bid == bo1.price
    assert ob.best_ask is None
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
    assert ob.best_bid is None
    assert ob.best_ask == so1.price
    assert all([o.status == OrderStatus.Filled for o in buy_orders])
    assert so1.status == OrderStatus.PartiallyFilled
    
def test_constant_best_ask():
    ob = Orderbook("test")
    o = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Sell)
    assert ob.best_bid is None
    assert ob.best_ask == o.price
    submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000004"), side=Side.Sell)
    assert ob.best_ask == o.price
    submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000004"), side=Side.Sell)
    assert ob.best_ask == o.price
    assert ob.best_bid is None
    
def test_constant_best_bid():
    ob = Orderbook("test")
    o = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob.best_ask is None
    assert ob.best_bid == o.price
    submit_order(ob, price=Decimal("0.000000002"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob.best_bid == o.price
    submit_order(ob, price=Decimal("0.000000002"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob.best_bid == o.price
    assert ob.best_ask is None
    
def test_decreasing_best_ask():
    ob = Orderbook("test")
    o1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Sell)
    assert ob.best_bid is None
    assert ob.best_ask == o1.price
    o2 = submit_order(ob, price=Decimal("0.000000002"), qty=Decimal("0.000000004"), side=Side.Sell)
    assert ob.best_ask == o2.price
    o3 = submit_order(ob, price=Decimal("0.000000002"), qty=Decimal("0.000000004"), side=Side.Sell)
    assert ob.best_ask == o2.price
    
def test_increasing_best_bid():
    ob = Orderbook("test")
    o1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob.best_ask is None
    assert ob.best_bid == o1.price
    o2 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob.best_bid == o2.price
    o3 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert ob.best_bid == o2.price
    
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
    assert subscriber.cancel_rejects[bo.order_id][0].reject_code == RejectCode.OrderDoesNotExist
    
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
    assert subscriber.cancel_rejects[bo.order_id][0].reject_code == RejectCode.OrderDoesNotExist
    
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
    
def test_single_replace_reject():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    bo1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    ob.replace_order(bo1, new_price=Decimal("0.000000003"), new_qty=Decimal("0.000000004"))
    assert len(subscriber.replace_rejects) == 1
    assert len(subscriber.replace_rejects[bo1.order_id]) == 1
    assert subscriber.replace_rejects[bo1.order_id][0].reject_code == RejectCode.PriceOrQtyMustBeChanged
    
def test_single_replace_only_price():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    bo1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert len(subscriber.order_updates[bo1.order_id]) == 1
    ob.replace_order(bo1, new_price=Decimal("0.000000002"), new_qty=Decimal("0.000000004"))
    assert len(subscriber.order_updates[bo1.order_id]) == 2
    assert subscriber.order_updates[bo1.order_id][0].status == OrderStatus.Open
    assert subscriber.order_updates[bo1.order_id][1].status == OrderStatus.Open
    buy_orders = list(ob.in_order_buy_orders())
    assert buy_orders[0].price == Decimal("0.000000002")
    assert buy_orders[0].qty == Decimal("0.000000004")
    
def test_single_replace_only_qty():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    bo1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert len(subscriber.order_updates[bo1.order_id]) == 1
    ob.replace_order(bo1, new_price=Decimal("0.000000003"), new_qty=Decimal("0.000000005"))
    assert len(subscriber.order_updates[bo1.order_id]) == 2
    assert subscriber.order_updates[bo1.order_id][0].status == OrderStatus.Open
    assert subscriber.order_updates[bo1.order_id][1].status == OrderStatus.Open
    buy_orders = list(ob.in_order_buy_orders())
    assert buy_orders[0].price == Decimal("0.000000003")
    assert buy_orders[0].qty == Decimal("0.000000005")
    
def test_single_replace_both_price_qty():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    bo1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    assert len(subscriber.order_updates[bo1.order_id]) == 1
    ob.replace_order(bo1, new_price=Decimal("0.000000004"), new_qty=Decimal("0.000000005"))
    assert len(subscriber.order_updates[bo1.order_id]) == 2
    assert subscriber.order_updates[bo1.order_id][0].status == OrderStatus.Open
    assert subscriber.order_updates[bo1.order_id][1].status == OrderStatus.Open
    buy_orders = list(ob.in_order_buy_orders())
    assert buy_orders[0].price == Decimal("0.000000004")
    assert buy_orders[0].qty == Decimal("0.000000005")
    
def test_single_replace_qty_partially_filled_reject():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    bo1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    so1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000003"), side=Side.Sell)
    assert len(subscriber.order_updates[bo1.order_id]) == 2
    ob.replace_order(bo1, new_price=Decimal("0.000000003"), new_qty=Decimal("0.000000002"))
    assert subscriber.order_updates[bo1.order_id][0].status == OrderStatus.Open
    assert subscriber.order_updates[bo1.order_id][1].status == OrderStatus.PartiallyFilled
    assert len(subscriber.replace_rejects) == 1
    assert subscriber.replace_rejects[bo1.order_id][0].reject_code == RejectCode.NewQtyCantBeLessThanOpenQty
    buy_orders = list(ob.in_order_buy_orders())
    assert buy_orders[0].price == Decimal("0.000000003")
    assert buy_orders[0].qty == Decimal("0.000000004")
    
def test_single_replace_price_partially_filled():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    bo1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    so1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000003"), side=Side.Sell)
    assert len(subscriber.order_updates[bo1.order_id]) == 2
    ob.replace_order(bo1, new_price=Decimal("0.000000004"), new_qty=Decimal("0.000000004"))
    assert subscriber.order_updates[bo1.order_id][0].status == OrderStatus.Open
    assert subscriber.order_updates[bo1.order_id][1].status == OrderStatus.PartiallyFilled
    assert len(subscriber.replace_rejects) == 0
    buy_orders = list(ob.in_order_buy_orders())
    assert buy_orders[0].price == Decimal("0.000000004")
    assert buy_orders[0].qty == Decimal("0.000000004")
    
def test_single_replace_price_partially_filled_to_be_filled_buy_order():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    bo1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000004"), side=Side.Buy)
    so1 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000003"), side=Side.Sell)
    so2 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000003"), side=Side.Sell)
    assert subscriber.order_updates[bo1.order_id][0].status == OrderStatus.Open
    assert subscriber.order_updates[bo1.order_id][1].status == OrderStatus.PartiallyFilled
    assert len(subscriber.trades) == 1
    assert subscriber.trades[0].qty == Decimal("0.000000003")
    assert subscriber.trades[0].buy_order_id == bo1.order_id
    assert subscriber.trades[0].sell_order_id == so2.order_id
    buy_orders = list(ob.in_order_buy_orders())
    assert buy_orders[0].status == OrderStatus.PartiallyFilled
    ob.replace_order(bo1, new_price=Decimal("0.000000004"), new_qty=Decimal("0.000000004"))
    assert len(subscriber.trades) == 2
    assert subscriber.trades[0].qty == Decimal("0.000000003")
    assert subscriber.trades[1].qty == Decimal("0.000000001")
    assert subscriber.trades[1].buy_order_id == bo1.order_id
    assert subscriber.trades[1].sell_order_id == so1.order_id
    assert bo1.status == OrderStatus.Filled
    buy_orders = list(ob.in_order_buy_orders())
    assert len(buy_orders) == 0
    sell_orders = list(ob.in_order_sell_orders())
    assert len(sell_orders) == 1
    assert sell_orders[0] == so1
    assert so1.filled_qty == Decimal("0.000000001")
    assert so1.status == OrderStatus.PartiallyFilled
    assert so2.status == OrderStatus.Filled
    
def test_single_replace_price_partially_filled_to_be_filled_sell_order():
    ob = Orderbook("test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    so1 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000004"), side=Side.Sell)
    bo1 = submit_order(ob, price=Decimal("0.000000003"), qty=Decimal("0.000000003"), side=Side.Buy)
    bo2 = submit_order(ob, price=Decimal("0.000000004"), qty=Decimal("0.000000003"), side=Side.Buy)
    assert subscriber.order_updates[so1.order_id][0].status == OrderStatus.Open
    assert subscriber.order_updates[so1.order_id][1].status == OrderStatus.PartiallyFilled
    assert len(subscriber.trades) == 1
    assert subscriber.trades[0].qty == Decimal("0.000000003")
    assert subscriber.trades[0].buy_order_id == bo2.order_id
    assert subscriber.trades[0].sell_order_id == so1.order_id
    buy_orders = list(ob.in_order_sell_orders())
    assert buy_orders[0].status == OrderStatus.PartiallyFilled
    ob.replace_order(so1, new_price=Decimal("0.000000003"), new_qty=Decimal("0.000000004"))
    assert len(subscriber.trades) == 2
    assert subscriber.trades[0].qty == Decimal("0.000000003")
    assert subscriber.trades[1].qty == Decimal("0.000000001")
    assert subscriber.trades[1].buy_order_id == bo1.order_id
    assert subscriber.trades[1].sell_order_id == so1.order_id
    assert so1.status == OrderStatus.Filled
    buy_orders = list(ob.in_order_sell_orders())
    assert len(buy_orders) == 0
    sell_orders = list(ob.in_order_buy_orders())
    assert len(sell_orders) == 1
    assert sell_orders[0] == bo1
    assert bo1.filled_qty == Decimal("0.000000001")
    assert bo1.status == OrderStatus.PartiallyFilled
    assert bo2.status == OrderStatus.Filled
    
def test_random_insert():
    ob = Orderbook(symbol="test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    price_min = 1
    price_max = 10
    qty_min = 1
    qty_max = 10
    
    def create_random_order() -> Order:
        return Order(cl_ord_id=string_helper.generate_uuid(),
                     order_id=string_helper.generate_uuid(),
                     side=Side(random.randint(0, 1)),
                     price=Decimal(random.randint(price_min, price_max)),
                     qty=Decimal(random.randint(qty_min, qty_max)),
                     symbol="test")
        
    def create_expected_update(order: Order, status: OrderStatus, price: Optional[Decimal] = None, qty: Optional[Decimal] = None, filled_qty: Optional[Decimal] = None) -> ExpectedOrderUpdate:
        return ExpectedOrderUpdate(cl_ord_id=order.cl_ord_id,
                                   order_id=order.order_id,
                                   qty=order.qty if qty is None else qty,
                                   price=order.price if price is None else price,
                                   status=status,
                                   filled_qty=order.filled_qty if filled_qty is None else filled_qty)
        
    random_orders = [
        create_order(price=Decimal("4"), qty=Decimal("1"), side=Side.Buy),
        create_order(price=Decimal("3"), qty=Decimal("7"), side=Side.Sell),
        create_order(price=Decimal("7"), qty=Decimal("6"), side=Side.Sell),
        create_order(price=Decimal("8"), qty=Decimal("5"), side=Side.Buy),
        create_order(price=Decimal("4"), qty=Decimal("8"), side=Side.Buy),
        create_order(price=Decimal("9"), qty=Decimal("7"), side=Side.Sell),
        create_order(price=Decimal("8"), qty=Decimal("7"), side=Side.Buy),
        create_order(price=Decimal("2"), qty=Decimal("2"), side=Side.Sell),
        create_order(price=Decimal("6"), qty=Decimal("9"), side=Side.Sell),
        create_order(price=Decimal("5"), qty=Decimal("5"), side=Side.Buy),
        create_order(price=Decimal("6"), qty=Decimal("8"), side=Side.Sell),
        create_order(price=Decimal("3"), qty=Decimal("6"), side=Side.Sell),
        create_order(price=Decimal("3"), qty=Decimal("4"), side=Side.Sell),
        create_order(price=Decimal("6"), qty=Decimal("8"), side=Side.Sell),
        create_order(price=Decimal("5"), qty=Decimal("7"), side=Side.Sell),
        create_order(price=Decimal("4"), qty=Decimal("7"), side=Side.Buy),
        create_order(price=Decimal("3"), qty=Decimal("2"), side=Side.Buy),
        create_order(price=Decimal("9"), qty=Decimal("10"), side=Side.Buy),
        create_order(price=Decimal("10"), qty=Decimal("9"), side=Side.Sell),
        create_order(price=Decimal("5"), qty=Decimal("2"), side=Side.Sell),
        create_order(price=Decimal("6"), qty=Decimal("9"), side=Side.Sell),
        create_order(price=Decimal("2"), qty=Decimal("7"), side=Side.Sell),
        create_order(price=Decimal("1"), qty=Decimal("1"), side=Side.Buy),
        create_order(price=Decimal("7"), qty=Decimal("1"), side=Side.Buy),
        create_order(price=Decimal("4"), qty=Decimal("6"), side=Side.Buy),
        create_order(price=Decimal("5"), qty=Decimal("4"), side=Side.Sell),
        create_order(price=Decimal("3"), qty=Decimal("6"), side=Side.Buy),
        create_order(price=Decimal("7"), qty=Decimal("7"), side=Side.Sell),
        create_order(price=Decimal("9"), qty=Decimal("8"), side=Side.Sell),
        create_order(price=Decimal("5"), qty=Decimal("5"), side=Side.Sell),
        create_order(price=Decimal("10"), qty=Decimal("10"), side=Side.Buy),
    ]
    for i in range(1000):
        next_order = create_random_order()
        buy_orders = list(ob.in_order_buy_orders())
        sell_orders = list(ob.in_order_sell_orders())
        expected_updates: Set[ExpectedOrderUpdate] = set()
        buy_order_count_diff = 0
        sell_order_count_diff = 0
        expected_updates.add(create_expected_update(order=next_order, status=OrderStatus.Open))        
        related_orders = buy_orders if next_order.side == Side.Sell else sell_orders
        sell_order_count_diff += next_order.side == Side.Sell
        buy_order_count_diff += next_order.side == Side.Buy
        expected_fill_qty = Decimal("0")
        for existing_order in related_orders:
            if bk_decimal.epsilon_zero(next_order.qty - expected_fill_qty):
                break
            if (bk_decimal.epsilon_gte(next_order.price, existing_order.price) and next_order.side == Side.Buy) or\
                (bk_decimal.epsilon_gte(existing_order.price, next_order.price) and next_order.side == Side.Sell):
                # a trade will occur                    
                if bk_decimal.epsilon_lt(existing_order.open_qty, next_order.qty - expected_fill_qty):
                    trade_qty = existing_order.open_qty
                else:
                    trade_qty = next_order.qty - expected_fill_qty
                expected_fill_qty += trade_qty
                if bk_decimal.epsilon_equal(trade_qty, existing_order.open_qty):
                    expected_updates.add(create_expected_update(order=existing_order, status=OrderStatus.Filled, filled_qty=existing_order.qty))
                    buy_order_count_diff -= existing_order.side == Side.Buy
                    sell_order_count_diff -= existing_order.side == Side.Sell
                else:
                    expected_updates.add(create_expected_update(order=existing_order, status=OrderStatus.PartiallyFilled, filled_qty=existing_order.filled_qty + trade_qty))
                
                if bk_decimal.epsilon_equal(next_order.qty - expected_fill_qty, Decimal("0")):
                    expected_updates.add(create_expected_update(order=next_order, status=OrderStatus.Filled, filled_qty=next_order.qty))
                    buy_order_count_diff -= next_order.side == Side.Buy
                    sell_order_count_diff -= next_order.side == Side.Sell
                else:
                    expected_updates.add(create_expected_update(order=next_order, status=OrderStatus.PartiallyFilled, filled_qty=expected_fill_qty))
                        
        ob.submit_order(next_order)
        post_buy_orders = list(ob.in_order_buy_orders())
        post_sell_orders = list(ob.in_order_sell_orders())
        assert len(buy_orders) + buy_order_count_diff == len(post_buy_orders)
        assert len(sell_orders) + sell_order_count_diff == len(post_sell_orders)
        for order_id, updates in subscriber.order_updates.items():
            for update in updates:
                expected_update = ExpectedOrderUpdate(cl_ord_id=update.cl_ord_id,
                                                      order_id=update.order_id,
                                                      qty=update.qty,
                                                      price=update.price,
                                                      status=update.status,
                                                      filled_qty=update.filled_qty)
                expected_updates.remove(expected_update)
        assert len(expected_updates) == 0
        subscriber.order_updates.clear()
                    
                
    