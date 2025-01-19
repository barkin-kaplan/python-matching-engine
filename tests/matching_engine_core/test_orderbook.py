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
            raise NotImplementedError(f"Can't compare objects of type {type(self).__name__} and {type(self).__name__}")
        return (self.cl_ord_id == other.cl_ord_id and
                self.order_id == other.order_id and
                self.qty == other.qty and
                self.price == other.price and
                self.status == other.status and
                self.filled_qty == other.filled_qty)

@dataclass
class RejectModel:
    order_id: str
    reject_code: RejectCode
    
    def __hash__(self):
        return hash((self.order_id, self.reject_code))
    
    def __eq__(self, other: 'RejectModel'):
        if not isinstance(other, RejectModel):
            raise NotImplementedError(f"Can't compare objects of type {type(self).__name__} and {type(self).__name__}")
        
        return self.order_id == other.order_id and self.reject_code == other.reject_code
            

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
        related_rejects.append(RejectModel(order_copy.order_id, reject_code))
    
    def on_replace_reject(self, order: Order, reject_code: RejectCode):
        order_copy = copy.deepcopy(order)
        related_rejects = self.replace_rejects.get(order_copy.order_id)
        if related_rejects is None:
            related_rejects = list()
            self.replace_rejects[order_copy.order_id] = related_rejects
        related_rejects.append(RejectModel(order_copy.order_id, reject_code))
    
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
    assert subscriber.replace_rejects[bo1.order_id][0].reject_code == RejectCode.NewQtyCantBeLessThanOrEqualToFilledQty
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
    
def create_random_order() -> Order:
        return Order(cl_ord_id=string_helper.generate_uuid(),
                     order_id=string_helper.generate_uuid(),
                     side=Side(random.randint(0, 1)),
                     price=Decimal(random.randint(1, 10)),
                     qty=Decimal(random.randint(1, 10)),
                     symbol="test")
        
def create_expected_update(order: Order, status: OrderStatus, price: Optional[Decimal] = None, qty: Optional[Decimal] = None, filled_qty: Optional[Decimal] = None) -> ExpectedOrderUpdate:
    return ExpectedOrderUpdate(cl_ord_id=order.cl_ord_id,
                                order_id=order.order_id,
                                qty=order.qty if qty is None else qty,
                                price=order.price if price is None else price,
                                status=status,
                                filled_qty=order.filled_qty if filled_qty is None else filled_qty)
    

@dataclass
class ExpectedObOperations:
    buy_order_count_diff: int
    sell_order_count_diff: int
    expected_order_updates: Set[ExpectedOrderUpdate]
    expected_cancel_rejects: List[RejectModel]
    expected_replace_rejects: List[RejectModel]
    
    
def create_expected_diffs_after_insert(next_order: Order, ob: Orderbook, is_replace: bool = False, replace_price: Optional[Decimal] = None, replace_qty: Optional[Decimal] = None) -> ExpectedObOperations:
    def create_replace_reject(reject_code: RejectCode):
        return ExpectedObOperations(buy_order_count_diff=0,
                                            sell_order_count_diff=0,
                                            expected_order_updates=set(),
                                            expected_cancel_rejects=[],
                                            expected_replace_rejects=[RejectModel(next_order.order_id, reject_code)])
    # create a new order object like it is new insert
    if is_replace:
        if (replace_price is None or bk_decimal.epsilon_equal(next_order.price, replace_price)) and\
                (replace_qty is None or bk_decimal.epsilon_equal(replace_qty,next_order.qty)):
            return create_replace_reject(RejectCode.PriceOrQtyMustBeChanged)
        if replace_qty is not None and bk_decimal.epsilon_lte(replace_qty, next_order.filled_qty):
            return create_replace_reject(RejectCode.NewQtyCantBeLessThanOrEqualToFilledQty)
                
        next_order = copy.deepcopy(next_order)
        if replace_price is not None:
            next_order.price = replace_price
        if replace_qty is not None:
            next_order.qty = replace_qty
    buy_orders = list(ob.in_order_buy_orders())
    sell_orders = list(ob.in_order_sell_orders())
    expected_updates: Set[ExpectedOrderUpdate] = set()
    buy_order_count_diff = 0
    sell_order_count_diff = 0
    if is_replace:
        expected_updates.add(create_expected_update(order=next_order, status=next_order.status))  
        buy_order_count_diff -= next_order.side == Side.Buy 
        sell_order_count_diff -= next_order.side == Side.Sell
    else:
        expected_updates.add(create_expected_update(order=next_order, status=OrderStatus.Open))        
    related_orders = buy_orders if next_order.side == Side.Sell else sell_orders
    sell_order_count_diff += next_order.side == Side.Sell
    buy_order_count_diff += next_order.side == Side.Buy
    expected_fill_qty = next_order.filled_qty
    for existing_order in related_orders:
        if is_replace and existing_order.order_id == next_order.order_id:
            continue
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
        else:
            # if no trade occurs then it is not possible for this order to match with lower priority orders
            break
    
    ops = ExpectedObOperations(
        buy_order_count_diff=buy_order_count_diff,
        sell_order_count_diff=sell_order_count_diff,
        expected_order_updates=expected_updates,
        expected_cancel_rejects=[],
        expected_replace_rejects=[]        
    )
    
    return ops
    
def test_random_insert():
    ob = Orderbook(symbol="test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    order_count = 1000
    order_update_counts_by_status: Dict[OrderStatus, int] = dict()
    order_ids_partially_filled_at_some_points: Set[str] = set()
    
    for i in range(order_count):
        next_order = create_random_order()
        buy_orders = list(ob.in_order_buy_orders())
        sell_orders = list(ob.in_order_sell_orders())
        print(f"create_order(Decimal('{next_order.price}'), Decimal('{next_order.qty}'), Side.{'Sell' if next_order.side == Side.Sell else 'Buy'}),")
        expected_ob_operations = create_expected_diffs_after_insert(next_order, ob)
        buy_order_count_diff, sell_order_count_diff, expected_updates = expected_ob_operations.buy_order_count_diff, expected_ob_operations.sell_order_count_diff, expected_ob_operations.expected_order_updates
        ob.submit_order(next_order)
        post_buy_orders = list(ob.in_order_buy_orders())
        post_sell_orders = list(ob.in_order_sell_orders())
        assert len(buy_orders) + buy_order_count_diff == len(post_buy_orders)
        assert len(sell_orders) + sell_order_count_diff == len(post_sell_orders)
        for order_id, updates in subscriber.order_updates.items():
            for update in updates:
                if update.status == OrderStatus.PartiallyFilled:
                    order_ids_partially_filled_at_some_points.add(update.order_id)
                count = order_update_counts_by_status.get(update.status, 0)
                order_update_counts_by_status[update.status] = count + 1
                expected_update = ExpectedOrderUpdate(cl_ord_id=update.cl_ord_id,
                                                      order_id=update.order_id,
                                                      qty=update.qty,
                                                      price=update.price,
                                                      status=update.status,
                                                      filled_qty=update.filled_qty)
                expected_updates.remove(expected_update)
        assert len(expected_updates) == 0
        subscriber.order_updates.clear()
        
    buy_orders = list(ob.in_order_buy_orders())
    sell_orders = list(ob.in_order_sell_orders())
    assert order_update_counts_by_status[OrderStatus.Open] == order_count
    assert order_update_counts_by_status[OrderStatus.Filled] == order_count - len(buy_orders) - len(sell_orders)
    assert order_update_counts_by_status.get(OrderStatus.Canceled) is None
    assert len(order_ids_partially_filled_at_some_points) <= order_count
    assert len(order_ids_partially_filled_at_some_points) <= order_update_counts_by_status[OrderStatus.Filled]
    
    
def test_replace_fill_all_buy_orders():
    ob = Orderbook(symbol="test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    order_update_counts_by_status: Dict[OrderStatus, int] = dict()
    order_ids_partially_filled_at_some_points: Set[str] = set()
    ob.submit_order(Order(cl_ord_id=string_helper.generate_uuid(),
                          order_id=string_helper.generate_uuid(),
                          side=Side.Buy,
                          qty=Decimal("5"),
                          price=Decimal("3"),
                          symbol="test"))
    ob.submit_order(Order(cl_ord_id=string_helper.generate_uuid(),
                          order_id=string_helper.generate_uuid(),
                          side=Side.Buy,
                          qty=Decimal("6"),
                          price=Decimal("3"),
                          symbol="test"))
    
    ob.submit_order(Order(cl_ord_id=string_helper.generate_uuid(),
                          order_id=string_helper.generate_uuid(),
                          side=Side.Sell,
                          qty=Decimal("12"),
                          price=Decimal("5"),
                          symbol="test",
                          filled_qty=Decimal("9")))
    to_be_replaced = Order(cl_ord_id=string_helper.generate_uuid(),
                          order_id=string_helper.generate_uuid(),
                          side=Side.Sell,
                          qty=Decimal("8"),
                          price=Decimal("12"),
                          symbol="test",
                          filled_qty=Decimal("3"),
                          status=OrderStatus.PartiallyFilled)
    ob.submit_order(to_be_replaced)
    subscriber.order_updates.clear()
    buy_orders = list(ob.in_order_buy_orders())
    sell_orders = list(ob.in_order_sell_orders())
    # roll random to replace price first
    replace_price = Decimal("1")
    # roll random to replace qty first
    replace_qty = Decimal("14")
    expected_ob_operations = create_expected_diffs_after_insert(copy.deepcopy(to_be_replaced), ob, is_replace=True, replace_price=replace_price, replace_qty=replace_qty)
    buy_order_count_diff, sell_order_count_diff, expected_updates = expected_ob_operations.buy_order_count_diff, expected_ob_operations.sell_order_count_diff, expected_ob_operations.expected_order_updates
    expected_replace_rejects = expected_ob_operations.expected_replace_rejects        
    
    # print(f"create_order(Decimal('{next_order.price}'), Decimal('{next_order.qty}'), Side.{'Sell' if next_order.side == Side.Sell else 'Buy'}),")
    orig_order = copy.deepcopy(to_be_replaced)
    ob.replace_order(to_be_replaced, replace_price, replace_qty)
    post_buy_orders = list(ob.in_order_buy_orders())
    post_sell_orders = list(ob.in_order_sell_orders())
    assert len(buy_orders) + buy_order_count_diff == len(post_buy_orders)
    assert len(sell_orders) + sell_order_count_diff == len(post_sell_orders)
    for order_id, updates in subscriber.order_updates.items():
        for update in updates:
            if update.status == OrderStatus.PartiallyFilled:
                order_ids_partially_filled_at_some_points.add(update.order_id)
            count = order_update_counts_by_status.get(update.status, 0)
            order_update_counts_by_status[update.status] = count + 1
            expected_update = ExpectedOrderUpdate(cl_ord_id=update.cl_ord_id,
                                                    order_id=update.order_id,
                                                    qty=update.qty,
                                                    price=update.price,
                                                    status=update.status,
                                                    filled_qty=update.filled_qty)
            expected_updates.remove(expected_update)
            
    for order_id, replace_rejects in subscriber.replace_rejects.items():
        for replace_reject in replace_rejects:
            expected_replace_rejects.remove(replace_reject)
    assert len(expected_updates) == 0
    assert len(expected_replace_rejects) == 0
    assert order_update_counts_by_status.get(OrderStatus.Canceled) is None
    
def test_random_cancel():
    ob = Orderbook(symbol="test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    order_update_counts_by_status: Dict[OrderStatus, int] = dict()
    order_ids_partially_filled_at_some_points: Set[str] = set()
    initial_buy_order_count = 0
    min_order_count_at_level = 10
    max_order_count_at_level = 100
    # construct orderbook
    for price in range(1, 11):
        order_count_at_level = random.randint(min_order_count_at_level, max_order_count_at_level)
        initial_buy_order_count += order_count_at_level
        for i in range(order_count_at_level):
            qty = random.randint(1, 10)
            order = create_order(Decimal(price), Decimal(qty), Side.Buy)
            ob.submit_order(order)
            
    initial_sell_order_count = 0
    for price in range(11, 21):
        order_count_at_level = random.randint(min_order_count_at_level, max_order_count_at_level)
        initial_sell_order_count += order_count_at_level
        for i in range(order_count_at_level):
            qty = random.randint(1, 10)
            order = create_order(Decimal(price), Decimal(qty), Side.Sell)
            ob.submit_order(order) 
           
    buy_orders = list(ob.in_order_buy_orders())
    sell_orders = list(ob.in_order_sell_orders())
    assert len(buy_orders) == initial_buy_order_count
    assert len(sell_orders) == initial_sell_order_count
    
    subscriber.order_updates.clear()

    while True:
        buy_orders = list(ob.in_order_buy_orders())
        sell_orders = list(ob.in_order_sell_orders())
        if len(buy_orders) == 0 and len(sell_orders) == 0:
            break
        buy_order_count_diff, sell_order_count_diff, expected_updates, expected_cancel_rejects = 0, 0, set(), []
        if random.randint(1, 10) == 5:
            # send cancel to an unknown order
            next_order = create_order(Decimal("4"), Decimal("4"), Side.Buy)
            expected_cancel_rejects.append(RejectModel(next_order.order_id, RejectCode.OrderDoesNotExist))
        else:
            next_side = Side(random.randint(0, 1))
            if next_side == Side.Buy:
                if len(buy_orders) == 0:
                    continue
                next_order = copy.deepcopy(random.choice(buy_orders))
            else:
                if len(sell_orders) == 0:
                    continue
                next_order = copy.deepcopy(random.choice(sell_orders))
                
            buy_order_count_diff -= next_order.side == Side.Buy
            sell_order_count_diff -= next_order.side == Side.Sell
            expected_updates.add(create_expected_update(next_order, status=OrderStatus.Canceled))
            
        ob.cancel_order(next_order)
        post_buy_orders = list(ob.in_order_buy_orders())
        post_sell_orders = list(ob.in_order_sell_orders())
        
        assert len(buy_orders) + buy_order_count_diff == len(post_buy_orders)
        assert len(sell_orders) + sell_order_count_diff == len(post_sell_orders)
        for order_id, updates in subscriber.order_updates.items():
            for update in updates:
                count = order_update_counts_by_status.get(update.status, 0)
                order_update_counts_by_status[update.status] = count + 1
                expected_update = ExpectedOrderUpdate(cl_ord_id=update.cl_ord_id,
                                                      order_id=update.order_id,
                                                      qty=update.qty,
                                                      price=update.price,
                                                      status=update.status,
                                                      filled_qty=update.filled_qty)
                
                expected_updates.remove(expected_update)
        for order_id, cancel_rejects in subscriber.cancel_rejects.items():
            for cancel_reject in cancel_rejects:
                expected_cancel_rejects.remove(cancel_reject)
        assert len(expected_updates) == 0
        assert len(expected_cancel_rejects) == 0
        subscriber.order_updates.clear()
        subscriber.cancel_rejects.clear()
        
    buy_orders = list(ob.in_order_buy_orders())
    sell_orders = list(ob.in_order_sell_orders())
    assert len(buy_orders) == 0
    assert len(sell_orders) == 0
        
                    
def test_random_replace():
    ob = Orderbook(symbol="test")
    subscriber = MockTransSubscriber()
    ob.subscribe(subscriber)
    order_update_counts_by_status: Dict[OrderStatus, int] = dict()
    order_ids_partially_filled_at_some_points: Set[str] = set()
    initial_buy_order_count = 0
    min_order_count_at_level = 10
    max_order_count_at_level = 100
    # construct orderbook
    for price in range(1, 11):
        order_count_at_level = random.randint(min_order_count_at_level, max_order_count_at_level)
        initial_buy_order_count += order_count_at_level
        for i in range(order_count_at_level):
            qty = random.randint(1, 10)
            order = create_order(Decimal(price), Decimal(qty), Side.Buy)
            ob.submit_order(order)
            
    initial_sell_order_count = 0
    for price in range(11, 21):
        order_count_at_level = random.randint(min_order_count_at_level, max_order_count_at_level)
        initial_sell_order_count += order_count_at_level
        for i in range(order_count_at_level):
            qty = random.randint(1, 10)
            order = create_order(Decimal(price), Decimal(qty), Side.Sell)
            ob.submit_order(order) 
           
    buy_orders = list(ob.in_order_buy_orders())
    sell_orders = list(ob.in_order_sell_orders())
    assert len(buy_orders) == initial_buy_order_count
    assert len(sell_orders) == initial_sell_order_count
    
    replace_count = 1000
    subscriber.order_updates.clear()
    while replace_count > 0:
        buy_orders = copy.deepcopy(list(ob.in_order_buy_orders()))
        sell_orders = copy.deepcopy(list(ob.in_order_sell_orders()))
        buy_order_count_diff, sell_order_count_diff, expected_updates, expected_cancel_rejects, expected_replace_rejects = 0, 0, set(), [], []
        if random.randint(1, 10) == 5:
            # send replace to an unknown order
            next_order = create_order(Decimal("4"), Decimal("4"), Side.Buy)
            replace_price = Decimal("5")
            replace_qty = Decimal("6")
            expected_replace_rejects.append(RejectModel(next_order.order_id, RejectCode.OrderDoesNotExist))
        else:
            next_side = Side(random.randint(0, 1))
            if next_side == Side.Buy:
                if len(buy_orders) == 0:
                    continue
                next_order = copy.deepcopy(random.choice(buy_orders))
            else:
                if len(sell_orders) == 0:
                    continue
                next_order = copy.deepcopy(random.choice(sell_orders))
            
            # roll random to replace price first
            replace_price = Decimal(random.randint(1, 21)) if random.randint(1, 2) == 1 else None
            # roll random to replace qty first
            replace_qty = Decimal(random.randint(1, 21)) if random.randint(1, 2) == 1 else None
            expected_ob_operations = create_expected_diffs_after_insert(copy.deepcopy(next_order), ob, is_replace=True, replace_price=replace_price, replace_qty=replace_qty)
            buy_order_count_diff, sell_order_count_diff, expected_updates = expected_ob_operations.buy_order_count_diff, expected_ob_operations.sell_order_count_diff, expected_ob_operations.expected_order_updates
            expected_replace_rejects = expected_ob_operations.expected_replace_rejects        
            
        ob.replace_order(next_order, replace_price, replace_qty)
        post_buy_orders = list(ob.in_order_buy_orders())
        post_sell_orders = list(ob.in_order_sell_orders())
        
        assert len(buy_orders) + buy_order_count_diff == len(post_buy_orders)
        assert len(sell_orders) + sell_order_count_diff == len(post_sell_orders)
        for order_id, updates in subscriber.order_updates.items():
            for update in updates:
                if update.status == OrderStatus.PartiallyFilled:
                    order_ids_partially_filled_at_some_points.add(update.order_id)
                count = order_update_counts_by_status.get(update.status, 0)
                order_update_counts_by_status[update.status] = count + 1
                expected_update = ExpectedOrderUpdate(cl_ord_id=update.cl_ord_id,
                                                      order_id=update.order_id,
                                                      qty=update.qty,
                                                      price=update.price,
                                                      status=update.status,
                                                      filled_qty=update.filled_qty)
                expected_updates.remove(expected_update)
        for order_id, replace_rejects in subscriber.replace_rejects.items():
            for replace_reject in replace_rejects:
                expected_replace_rejects.remove(replace_reject)
        assert len(expected_updates) == 0
        assert len(expected_replace_rejects) == 0
        subscriber.order_updates.clear()
        subscriber.replace_rejects.clear()
        replace_count -= 1
        
    buy_orders = list(ob.in_order_buy_orders())
    sell_orders = list(ob.in_order_sell_orders())
    order_update_counts_by_status.get(OrderStatus.Filled)
    if order_update_counts_by_status is not None:
        assert order_update_counts_by_status[OrderStatus.Filled] == initial_buy_order_count + initial_sell_order_count - len(buy_orders) - len(sell_orders)
    assert order_update_counts_by_status.get(OrderStatus.Canceled) is None
                    
