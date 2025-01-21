from decimal import Decimal
import random
import time
from typing import List
from helper import string_helper
from matching_engine_core.models.order import Order
from matching_engine_core.models.order_status import OrderStatus
from matching_engine_core.models.side import Side
from matching_engine_core.orderbook import Orderbook

insert_count_small = 1000
insert_count_medium = 10000
insert_count_large = 100000

def insert_count_test(count: int) -> float:
    ob = Orderbook("TEST")
    # initialize orders beforehand
    orders: List[Order] = []
    for i in range(count):
        order = Order(
            cl_ord_id=string_helper.generate_uuid(),
            order_id=string_helper.generate_uuid(),
            side=Side.Buy if random.randint(1, 2) == 1 else Side.Sell,
            qty=Decimal(random.randint(1, 10000)),
            price=Decimal(random.randint(1, 10000)),
            symbol="TEST"
        )
        
        orders.append(order)
    
    start = time.time()
    for order in orders:
        ob.submit_order(order)
        
    end = time.time()
    assert len([o for o in orders if o.status == OrderStatus.Filled]) + len(list(ob.in_order_buy_orders())) + len(list(ob.in_order_sell_orders())) == count
    return end - start

def insert_count_small_test():
    duration = insert_count_test(insert_count_small)
    print(f"Insert small (count: {insert_count_small}) took {duration} seconds")
    
def insert_count_medium_test():
    duration = insert_count_test(insert_count_medium)
    print(f"Insert medium (count: {insert_count_medium}) took {duration} seconds")
    
def insert_count_large_test():
    duration = insert_count_test(insert_count_large)
    print(f"Insert large (count: {insert_count_large}) took {duration} seconds")
    
insert_count_small_test()
insert_count_medium_test()
insert_count_large_test()
    
    
