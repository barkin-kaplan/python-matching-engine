from decimal import Decimal
from typing import Generator, List, Optional, cast
from helper import bk_decimal
from helper.collections.mapped_doubly_queue import MappedDoublyQueue
from helper.collections.red_black_tree import RedBlackTree
from matching_engine_core.i_transaction_subscriber import ITransactionSubscriber
from matching_engine_core.models.order import Order
from matching_engine_core.models.side import Side
from matching_engine_core.models.trade import Trade


class Orderbook:
    def __init__(self, symbol: str):
        self.symbol = symbol
        # price levels should be n sorted order for fast inorder traversal, 
        # orders at a price level has priority based on time and should be removed in constant time with random access
        self._best_bid: Optional[Decimal] = None
        self._best_ask: Optional[Decimal] = None
        self._buy_levels: RedBlackTree[Decimal, MappedDoublyQueue[str, Order]] = RedBlackTree()
        self._sell_levels: RedBlackTree[Decimal, MappedDoublyQueue[str, Order]] = RedBlackTree()
        self._t_subs: List[ITransactionSubscriber] = []
        
    def _publish_trade(self, trade: Trade):
        for sub in self._t_subs:
            sub.on_trade(trade)
            
    def subscribe(self, sub: ITransactionSubscriber):
        if sub not in self._t_subs:
            self._t_subs.append(sub)
            
    def in_order_buy_orders(self) -> Generator[Order, None, None]:
        for price, orders in self._buy_levels.reverse_order():
            for order_id, order in orders.traverse():
                yield order
                
    def in_order_sell_orders(self) -> Generator[Order, None, None]:
        for price, orders in self._sell_levels.in_order():
            for order_id, order in orders.traverse():
                yield order
        
    def submit_order(self, order: Order):
        if order.side == Side.Buy:
            if self._best_ask is not None and order.price >= self._best_ask:
                # check active matches
                for price, sell_orders in self._sell_levels.in_order():
                    if price <= order.price:
                        while not sell_orders.is_empty and not bk_decimal.is_epsilon_equal(order.open_qty, Decimal("0")):
                            sell_order = cast(Order, sell_orders.peek())
                            if sell_order.open_qty >= order.open_qty:
                                trade_qty = order.open_qty
                            else:
                                trade_qty = sell_order.open_qty
                            sell_order.open_qty -= trade_qty
                            order.open_qty -= trade_qty
                            trade = Trade(active_side=Side.Buy,
                                          buy_order_id=order.order_id,
                                          sell_order_id=order.order_id,
                                          qty=trade_qty,
                                          price=sell_order.price)
                            self._publish_trade(trade)
                            
                            if bk_decimal.is_epsilon_equal(sell_order.open_qty, Decimal("0")):
                                sell_orders.dequeue()
            if not bk_decimal.is_epsilon_equal(order.open_qty, Decimal("0")):
                # place order into orderbook
                orders = self._buy_levels[order.price]
                if orders is None:
                    orders = MappedDoublyQueue()
                    self._buy_levels[order.price] = orders
                    
                if self._best_bid is None or order.price > self._best_bid:
                    self._best_bid = order.price
                orders.enqueue(order.order_id, order)
                
        else:
            if self._best_bid is not None and order.price <= self._best_bid:
                # check active matches
                for price, buy_orders in self._buy_levels.reverse_order():
                    if price >= order.price:
                        while not buy_orders.is_empty and not bk_decimal.is_epsilon_equal(order.open_qty, Decimal("0")):
                            buy_order = cast(Order, buy_orders.peek())
                            if buy_order.open_qty >= order.open_qty:
                                trade_qty = order.open_qty
                            else:
                                trade_qty = buy_order.open_qty
                            buy_order.open_qty -= trade_qty
                            order.open_qty -= trade_qty
                            trade = Trade(active_side=Side.Sell,
                                          buy_order_id=order.order_id,
                                          sell_order_id=order.order_id,
                                          qty=trade_qty,
                                          price=buy_order.price)
                            self._publish_trade(trade)
                            
                            if bk_decimal.is_epsilon_equal(buy_order.open_qty, Decimal("0")):
                                buy_orders.dequeue()
            if not bk_decimal.is_epsilon_equal(order.open_qty, Decimal("0")):
                # place order into orderbook
                orders = self._sell_levels[order.price]
                if orders is None:
                    orders = MappedDoublyQueue()
                    self._sell_levels[order.price] = orders
                    
                if self._best_ask is None or order.price < self._best_ask:
                    self._best_ask = order.price
                orders.enqueue(order.order_id, order)
                
                            
            
        
        