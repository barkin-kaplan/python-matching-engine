from dataclasses import dataclass
from decimal import Decimal
import random
import time
from typing import Dict, List, Optional
from helper import string_helper
from matching_engine_core.models.order import Order
from matching_engine_core.models.order_status import OrderStatus
from matching_engine_core.models.side import Side
from matching_engine_core.orderbook import Orderbook

# Notes about performances: red black trees have log(n) operation times(insert, delete, search) and Orderbook implementation uses red black trees for price levels. 
# The following can be deduced about performance of order submission
# 1) For same amount of price levels, increasing order count increases execution times linearly,
# 2) For same amount of orders, increasing price level count increases execution times logarithmically where base of the logarithm is 2

SMALL = 1000
MEDIUM = 10000
LARGE = 100000
QTY_MIN = 90
QTY_MAX = 100

@dataclass
class ReplaceModel:
    order: Order
    new_price: Optional[Decimal]
    new_qty: Optional[Decimal]
    
def get_random_qty() -> Decimal:
    return Decimal(random.randint(QTY_MIN, QTY_MAX))

def initialize_orders(count: int, price_range: int, no_matching: bool = False) -> List[Order]:
    orders: List[Order] = []
    if no_matching:
        buy_max_price = price_range // 2
        sell_min_price = buy_max_price + 1
        for i in range(count // 2):
            order = Order(
                cl_ord_id=string_helper.generate_uuid(),
                order_id=string_helper.generate_uuid(),
                side=Side.Buy,
                qty=get_random_qty(),
                price=Decimal(random.randint(1, buy_max_price)),
                symbol="TEST"
            )
            orders.append(order)
            
        for i in range(count // 2):
            order = Order(
                cl_ord_id=string_helper.generate_uuid(),
                order_id=string_helper.generate_uuid(),
                side=Side.Sell,
                qty=get_random_qty(),
                price=Decimal(random.randint(sell_min_price, price_range)),
                symbol="TEST"
            )
            orders.append(order)
            
    else:
        for i in range(count):
            order = Order(
                cl_ord_id=string_helper.generate_uuid(),
                order_id=string_helper.generate_uuid(),
                side=Side.Buy if random.randint(1, 2) == 1 else Side.Sell,
                qty=get_random_qty(),
                price=Decimal(random.randint(1, price_range)),
                symbol="TEST"
            )
            
            orders.append(order)
    
    return orders

def prepare_replace_items(orders: List[Order], replace_count: int, price_range: int) -> List[ReplaceModel]:
    replace_items: List[ReplaceModel] = []
    for i in range(replace_count):
        order = random.choice(orders)
        if random.randint(1, 2) == 1:
            new_price = Decimal(random.randint(1, price_range))
        else:
            new_price = None
            
        if random.randint(1, 2) == 1:
            new_qty = get_random_qty()
        else:
            new_qty = None
            
        replace_items.append(ReplaceModel(order, new_price, new_qty))
        
    return replace_items

def prepare_cancels(orders: List[Order], cancel_count: int) -> List[Order]:
    cancel_orders: List[Order] = []
    for i in range(cancel_count):
        cancel_orders.append(random.choice(orders))
        
    return cancel_orders

def insert_test(count: int, price_range: int) -> float:
    ob = Orderbook("TEST")
    # initialize orders beforehand
    orders = initialize_orders(count, price_range)
    
    start = time.time()
    for order in orders:
        ob.submit_order(order)
        
    end = time.time()
    assert len([o for o in orders if o.status == OrderStatus.Filled]) + len(list(ob.in_order_buy_orders())) + len(list(ob.in_order_sell_orders())) == count
    return end - start

def insert_small_test():
    # insert count is small over different price ranges
    duration = insert_test(SMALL, SMALL)
    print(f"Insert small (count: {SMALL}) on small price range([1,{SMALL}]) took {duration} seconds")
    duration = insert_test(SMALL, MEDIUM)
    print(f"Insert small (count: {SMALL}) on medium price range([1,{MEDIUM}]) took {duration} seconds")
    duration = insert_test(SMALL, LARGE)
    print(f"Insert small (count: {SMALL}) on large price range([1,{LARGE}]) took {duration} seconds")
    
def insert_medium_test():
    duration = insert_test(MEDIUM, SMALL)
    print(f"Insert medium (count: {MEDIUM}) on small price range([1,{SMALL}]) took {duration} seconds")
    duration = insert_test(MEDIUM, MEDIUM)
    print(f"Insert medium (count: {MEDIUM}) on medium price range([1,{MEDIUM}]) took {duration} seconds")
    duration = insert_test(MEDIUM, LARGE)
    print(f"Insert medium (count: {MEDIUM}) on large price range([1,{LARGE}]) took {duration} seconds")
    
def insert_large_test():
    duration = insert_test(LARGE, SMALL)
    print(f"Insert large (count: {LARGE}) on small price range([1,{SMALL}]) took {duration} seconds")
    duration = insert_test(LARGE, MEDIUM)
    print(f"Insert large (count: {LARGE}) on medium price range([1,{MEDIUM}]) took {duration} seconds")
    duration = insert_test(LARGE, LARGE)
    print(f"Insert large (count: {LARGE}) on large price range([1,{LARGE}]) took {duration} seconds")
    
def replace_test_unit(replace_count: int, insert_count: int, price_range: int) -> float:
    ob = Orderbook("TEST")
    orders = initialize_orders(insert_count, price_range, no_matching=True)
    orders_by_id: Dict[str, Order] = dict()
    for order in orders:
        ob.submit_order(order)
        orders_by_id[order.order_id] = order
    
    replace_items = prepare_replace_items(orders, replace_count, price_range)
    
    start = time.time()
    for replace_item in replace_items:
        ob.replace_order(replace_item.order, replace_item.new_price, replace_item.new_qty)
    end = time.time()
    
    assert len([o for o in orders if o.status == OrderStatus.Filled]) + len(list(ob.in_order_buy_orders())) + len(list(ob.in_order_sell_orders())) == insert_count
    return end - start
        
def cancel_test_unit(cancel_count: int, insert_count: int, price_range: int) -> float:
    ob = Orderbook("TEST")
    orders = initialize_orders(insert_count, price_range, no_matching=True)
    orders_by_id: Dict[str, Order] = dict()
    for order in orders:
        ob.submit_order(order)
        orders_by_id[order.order_id] = order
    
    cancel_orders = prepare_cancels(orders, cancel_count)
    
    start = time.time()
    for order in cancel_orders:
        ob.cancel_order(order)
    end = time.time()
    
    assert len([o for o in orders if o.status == OrderStatus.Canceled]) + len(list(ob.in_order_buy_orders())) + len(list(ob.in_order_sell_orders())) == insert_count
    return end - start
    
    
def replace_test(count: int, count_verbal: str):
    duration = replace_test_unit(count, SMALL, SMALL)
    print(f"Replace {count_verbal}(count: {count}) for small order count({SMALL}), for small price range (([1,{SMALL}])) took {duration} seconds")
    duration = replace_test_unit(count, SMALL, MEDIUM)
    print(f"Replace {count_verbal}(count: {count}) for small order count({SMALL}), for medium price range (([1,{MEDIUM}])) took {duration} seconds")
    duration = replace_test_unit(count, SMALL, LARGE)
    print(f"Replace {count_verbal}(count: {count}) for small order count({SMALL}), for large price range (([1,{LARGE}])) took {duration} seconds")
    duration = replace_test_unit(count, MEDIUM, SMALL)
    print(f"Replace {count_verbal}(count: {count}) for medium order count({MEDIUM}), for small price range (([1,{SMALL}])) took {duration} seconds")
    duration = replace_test_unit(count, MEDIUM, MEDIUM)
    print(f"Replace {count_verbal}(count: {count}) for medium order count({MEDIUM}), for medium price range (([1,{MEDIUM}])) took {duration} seconds")
    duration = replace_test_unit(count, MEDIUM, LARGE)
    print(f"Replace {count_verbal}(count: {count}) for medium order count({MEDIUM}), for large price range (([1,{LARGE}])) took {duration} seconds")
    duration = replace_test_unit(count, LARGE, SMALL)
    print(f"Replace {count_verbal}(count: {count}) for large order count({LARGE}), for small price range (([1,{SMALL}])) took {duration} seconds")
    duration = replace_test_unit(count, LARGE, MEDIUM)
    print(f"Replace {count_verbal}(count: {count}) for large order count({LARGE}), for medium price range (([1,{MEDIUM}])) took {duration} seconds")
    duration = replace_test_unit(count, LARGE, LARGE)
    print(f"Replace {count_verbal}(count: {count}) for large order count({LARGE}), for large price range (([1,{LARGE}])) took {duration} seconds")
    
def cancel_test(count: int, count_verbal: str):
    duration = cancel_test_unit(count, SMALL, SMALL)
    print(f"Cancel {count_verbal}(count: {count}) for small order count({SMALL}), for small price range (([1,{SMALL}])) took {duration} seconds")
    duration = cancel_test_unit(count, SMALL, MEDIUM)
    print(f"Cancel {count_verbal}(count: {count}) for small order count({SMALL}), for medium price range (([1,{MEDIUM}])) took {duration} seconds")
    duration = cancel_test_unit(count, SMALL, LARGE)
    print(f"Cancel {count_verbal}(count: {count}) for small order count({SMALL}), for large price range (([1,{LARGE}])) took {duration} seconds")
    duration = cancel_test_unit(count, MEDIUM, SMALL)
    print(f"Cancel {count_verbal}(count: {count}) for medium order count({MEDIUM}), for small price range (([1,{SMALL}])) took {duration} seconds")
    duration = cancel_test_unit(count, MEDIUM, MEDIUM)
    print(f"Cancel {count_verbal}(count: {count}) for medium order count({MEDIUM}), for medium price range (([1,{MEDIUM}])) took {duration} seconds")
    duration = cancel_test_unit(count, MEDIUM, LARGE)
    print(f"Cancel {count_verbal}(count: {count}) for medium order count({MEDIUM}), for large price range (([1,{LARGE}])) took {duration} seconds")
    duration = cancel_test_unit(count, LARGE, SMALL)
    print(f"Cancel {count_verbal}(count: {count}) for large order count({LARGE}), for small price range (([1,{SMALL}])) took {duration} seconds")
    duration = cancel_test_unit(count, LARGE, MEDIUM)
    print(f"Cancel {count_verbal}(count: {count}) for large order count({LARGE}), for medium price range (([1,{MEDIUM}])) took {duration} seconds")
    duration = cancel_test_unit(count, LARGE, LARGE)
    print(f"Cancel {count_verbal}(count: {count}) for large order count({LARGE}), for large price range (([1,{LARGE}])) took {duration} seconds")
    
    
    
insert_small_test()
insert_medium_test()
insert_large_test()
replace_test(SMALL, "small")
replace_test(MEDIUM, "medium")
replace_test(LARGE, "large")
cancel_test(SMALL, "small")
cancel_test(MEDIUM, "medium")
cancel_test(LARGE, "large")
    
