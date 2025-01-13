from decimal import Decimal
from typing import Generator, List, Optional, cast
from helper import bk_decimal
from helper.collections.mapped_doubly_queue import MappedDoublyQueue
from helper.collections.red_black_tree import RedBlackTree
from matching_engine_core.i_transaction_subscriber import ITransactionSubscriber
from matching_engine_core.models.order import Order
from matching_engine_core.models.order_status import OrderStatus
from matching_engine_core.models.reject_codes import RejectCode
from matching_engine_core.models.side import Side
from matching_engine_core.models.trade import Trade


class Orderbook:
    def __init__(self, symbol: str):
        self.symbol = symbol
        # price levels should be n sorted order for fast inorder traversal, 
        # orders at a price level has priority based on time and should be removed in constant time with random access
        self._buy_levels: RedBlackTree[Decimal, MappedDoublyQueue[str, Order]] = RedBlackTree()
        self._sell_levels: RedBlackTree[Decimal, MappedDoublyQueue[str, Order]] = RedBlackTree()
        self._t_subs: List[ITransactionSubscriber] = []
        
    @property
    def best_bid(self) -> Optional[Decimal]:
        return self._buy_levels.maximum
    
    @property
    def best_ask(self) -> Optional[Decimal]:
        return self._sell_levels.minimum
        
    def _publish_trade(self, trade: Trade):
        for sub in self._t_subs:
            sub.on_trade(trade)
            
    def _publish_order_update(self, order: Order):
        for sub in self._t_subs:
            sub.on_order_update(order)
            
    def _publish_cancel_reject(self, order: Order, reject_code: RejectCode):
        for sub in self._t_subs:
            sub.on_cancel_reject(order, reject_code)
            
    def _publish_replace_reject(self, order: Order, reject_code: RejectCode):
        for sub in self._t_subs:
            sub.on_replace_reject(order, reject_code)
            
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
        # when replacing order status may be equal to PartiallyFilled
        if order.status == OrderStatus.PendingNew:
            order.status = OrderStatus.Open
        self._publish_order_update(order)
        if order.side == Side.Buy:
            if self.best_ask is not None and order.price >= self.best_ask:
                # check active matches
                for price, sell_orders in self._sell_levels.in_order():
                    if price <= order.price:
                        while not sell_orders.is_empty and not bk_decimal.epsilon_equal(order.open_qty, Decimal("0")):
                            sell_order = cast(Order, sell_orders.peek())
                            if sell_order.open_qty >= order.open_qty:
                                trade_qty = order.open_qty
                            else:
                                trade_qty = sell_order.open_qty
                            sell_order.filled_qty += trade_qty
                            order.filled_qty += trade_qty
                            trade = Trade(active_side=Side.Buy,
                                          buy_order_id=order.order_id,
                                          sell_order_id=sell_order.order_id,
                                          qty=trade_qty,
                                          price=sell_order.price)
                            sell_order.update_state_after_transaction()
                            order.update_state_after_transaction()
                            self._publish_order_update(sell_order)
                            self._publish_order_update(order)
                            self._publish_trade(trade)
                            
                            if bk_decimal.epsilon_equal(sell_order.open_qty, Decimal("0")):
                                sell_orders.dequeue()
                                if sell_orders.is_empty:
                                    del self._sell_levels[sell_order.price]
            if not bk_decimal.epsilon_equal(order.open_qty, Decimal("0")):
                # place order into orderbook
                orders = self._buy_levels[order.price]
                if orders is None:
                    orders = MappedDoublyQueue()
                    self._buy_levels[order.price] = orders
                orders.enqueue(order.order_id, order)
                
        else:
            if self.best_bid is not None and order.price <= self.best_bid:
                # check active matches
                for price, buy_orders in self._buy_levels.reverse_order():
                    if price >= order.price:
                        while not buy_orders.is_empty and not bk_decimal.epsilon_equal(order.open_qty, Decimal("0")):
                            buy_order = cast(Order, buy_orders.peek())
                            if buy_order.open_qty >= order.open_qty:
                                trade_qty = order.open_qty
                            else:
                                trade_qty = buy_order.open_qty
                            buy_order.filled_qty += trade_qty
                            order.filled_qty += trade_qty
                            trade = Trade(active_side=Side.Sell,
                                          buy_order_id=buy_order.order_id,
                                          sell_order_id=order.order_id,
                                          qty=trade_qty,
                                          price=buy_order.price)
                            buy_order.update_state_after_transaction()
                            order.update_state_after_transaction()
                            self._publish_order_update(buy_order)
                            self._publish_order_update(order)
                            self._publish_trade(trade)
                            
                            if bk_decimal.epsilon_equal(buy_order.open_qty, Decimal("0")):
                                buy_orders.dequeue()
                                if buy_orders.is_empty:
                                    del self._buy_levels[buy_order.price]
            if not bk_decimal.epsilon_equal(order.open_qty, Decimal("0")):
                # place order into orderbook
                orders = self._sell_levels[order.price]
                if orders is None:
                    orders = MappedDoublyQueue()
                    self._sell_levels[order.price] = orders
                    
                orders.enqueue(order.order_id, order)
                
    def _cancel_without_publish(self, order: Order) -> Optional[RejectCode]:
        orders = None
        if order.side == Side.Buy:
            orders = self._buy_levels[order.price]
        else:
            orders = self._sell_levels[order.price]
            
        if orders is None:
            return RejectCode.OrderDoesNotExist
        
        delete_result = orders.delete(order.order_id)
        if not delete_result:
            return RejectCode.OrderDoesNotExist
        
        return None
                
    def cancel_order(self, order: Order):
        result = self._cancel_without_publish(order)
        if isinstance(result, RejectCode):
            self._publish_cancel_reject(order, result)
        else:
            order.status = OrderStatus.Canceled
            self._publish_order_update(order)
            
    def replace_order(self, order: Order, new_price: Decimal, new_qty: Decimal):
        if bk_decimal.epsilon_equal(order.price, new_price) and bk_decimal.epsilon_equal(order.qty, new_qty):
            self._publish_replace_reject(order, RejectCode.PriceOrQtyMustBeChanged)
            return 
        if bk_decimal.epsilon_lt(new_qty, order.filled_qty):
            self._publish_replace_reject(order, RejectCode.NewQtyCantBeLessThanOpenQty)
            return
        result = self._cancel_without_publish(order)
        if isinstance(result, RejectCode):
            self._publish_replace_reject(order, result)
        else:
            order.price = new_price
            order.qty = new_qty
            self.submit_order(order)
            
        
            
                
                
                            
            
        
        