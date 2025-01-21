from dataclasses import dataclass
from decimal import Decimal
import tkinter as tk
from tkinter import ttk
from typing import Dict, List, Optional

from helper import string_helper
from matching_engine_core.i_transaction_subscriber import ITransactionSubscriber
from matching_engine_core.models.order import Order
from matching_engine_core.models.reject_codes import RejectCode
from matching_engine_core.models.side import Side
from matching_engine_core.models.trade import Trade
from matching_engine_core.orderbook import Orderbook

@dataclass
class PriceLevel:
    qty: Decimal
    total_qty: Decimal
    price: Decimal

class OrderbookGUI(ITransactionSubscriber):
    def __init__(self):
        self.orderbook = Orderbook("TEST")
        self.orderbook.subscribe(self)
        self.trades: List[Trade] = []
        # UI related
        self.root = tk.Tk()
        self.root.title("BK Orderbook Demo")
        self.root.geometry("1100x500")
        self.bold_buy_color = self.rgb_to_hex(144, 238, 144)  # Light Green
        self.bold_sell_color = self.rgb_to_hex(240, 128, 128)  # Light Coral
        self.light_buy_color = self.rgb_to_hex(184, 255, 184)
        self.light_sell_color = self.rgb_to_hex(255, 168, 168)
        self.replace_bg = self.rgb_to_hex(118, 206, 219)
        self.cancel_bg = self.rgb_to_hex(212, 138, 59)
        self.buy_color_alternates = ["buy_light", "buy_bold"]
        self.sell_color_alternates = ["sell_light", "sell_bold"]
        self.top_frame = tk.Frame(self.root)
        self.top_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.create_orderbook_component()
        self.create_order_entry_components()
        self.create_trades_component()
        self.create_open_orders_component()
        self._selected_order_id: Optional[str] = None      
        self.open_orders: Dict[str, Order] = dict()
        
    @property
    def selected_order_id(self) -> Optional[str]:
        return self._selected_order_id
    
    @selected_order_id.setter
    def selected_order_id(self, value):
        if value != self._selected_order_id:
            self._selected_order_id = value
            self.refresh_selected_order_id()
            
    @property
    def selected_order(self) -> Optional[Order]:
        if self.selected_order_id is None:
            return None
        
        return self.open_orders[self.selected_order_id]

    @staticmethod
    def rgb_to_hex(r, g, b):
        return f'#{r:02x}{g:02x}{b:02x}'
    
    @staticmethod
    def set_entry_text(entry, text: str):
        # Clear existing text and set new text
        entry.delete(0, tk.END)  # Clear the Entry
        entry.insert(0, text)  # Insert new text
    
    @staticmethod
    def validate_decimal(new_value):
        """
        Validation function to allow only numbers and a single dot.
        """
        if new_value == "":  # Allow clearing the entry
            return True
        try:
            # Allow valid float format with at most one dot
            if new_value.count(".") > 1:
                return False
            float(new_value)  # Check if it can be converted to a float
            return True
        except ValueError:
            return False
    
    def block_selection(self, tree, event):
        # Cancel the default selection behavior
        tree.selection_remove(tree.selection())
        return "break"
    
    def create_orderbook_component(self):
        # Create a parent frame for the order book components
        buy_levels_frame = tk.Frame(self.top_frame)
        buy_levels_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)
        label = tk.Label(buy_levels_frame, text="Buy Levels")
        label.pack(side=tk.TOP)
        # Create Treeview
        columns = ("Price", "Quantity", "Total")
        buy_tree = ttk.Treeview(buy_levels_frame, columns=columns, show="headings", height=10)
        for col in columns:
            buy_tree.heading(col, text=col)
            buy_tree.column(col, width=80, anchor=tk.CENTER)
        buy_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)
        
        buy_tree.tag_configure("buy_light", background=self.light_buy_color)
        buy_tree.tag_configure("buy_bold", background=self.bold_buy_color)
        buy_tree.bind("<Button-1>", lambda event: self.block_selection(buy_tree, event))
        self.buy_tree = buy_tree
        
        sell_levels_frame = tk.Frame(self.top_frame)
        sell_levels_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)
        label = tk.Label(sell_levels_frame, text="Sell Levels")
        label.pack(side=tk.TOP)
        columns = ("Price", "Quantity", "Total")
        sell_tree = ttk.Treeview(sell_levels_frame, columns=columns, show="headings", height=10)
        for col in columns:
            sell_tree.heading(col, text=col)
            sell_tree.column(col, width=80, anchor=tk.CENTER)
        sell_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)
        sell_tree.tag_configure("sell_light", background=self.light_sell_color)
        sell_tree.tag_configure("sell_bold", background=self.bold_sell_color)
        sell_tree.bind("<Button-1>", lambda event: self.block_selection(sell_tree, event))
        self.sell_tree = sell_tree
        
    def create_order_entry_components(self):
        frame = tk.Frame(self.root)
        frame.pack(pady=10, fill=tk.X, side=tk.TOP)
        tk.Label(frame, text="Price:").grid(row=0, column=0, padx=5, pady=5)
        self.insert_price_entry = tk.Entry(frame)
        self.insert_price_entry.grid(row=0, column=1, padx=5)
        vcmd = (self.root.register(self.validate_decimal), "%P")
        self.insert_price_entry.configure(validate="key", validatecommand=vcmd)

        tk.Label(frame, text="Quantity:").grid(row=0, column=2, padx=5, pady=5)
        self.insert_quantity_entry = tk.Entry(frame)
        self.insert_quantity_entry.grid(row=0, column=3, padx=5)
        vcmd = (self.root.register(self.validate_decimal), "%P")
        self.insert_quantity_entry.configure(validate="key", validatecommand=vcmd)

        tk.Button(frame, text="Buy", command=lambda :self.submit_order(Side.Buy), bg=self.light_buy_color).grid(row=0, column=4, padx=5)
        tk.Button(frame, text="Sell", command=lambda :self.submit_order(Side.Sell), bg=self.light_sell_color).grid(row=0, column=5, padx=5)
        selected_order_frame = tk.Frame(self.root)
        selected_order_frame.pack(pady=10, fill=tk.X, side=tk.TOP)
        tk.Label(selected_order_frame, text="Selected Order ID:").pack(side=tk.LEFT, padx=5)
        self.selected_order_id_label = tk.Label(selected_order_frame, text="")
        self.selected_order_id_label.pack(side=tk.LEFT)
        hint_frame = tk.Frame(self.root)
        hint_frame.pack(fill=tk.X, side=tk.TOP)
        tk.Label(hint_frame, text="Hint: select an open order by clicking on it from 'Open Orders' section").pack(side=tk.LEFT, padx=5)
        replace_frame = tk.Frame(self.root)
        replace_frame.pack(pady=10, fill=tk.X, side=tk.TOP)
        tk.Label(replace_frame, text="New Price:").grid(row=0, column=2, padx=5, pady=5)
        self.replace_price_entry = tk.Entry(replace_frame)
        self.replace_price_entry.grid(row=0, column=3, padx=5)
        vcmd = (self.root.register(self.validate_decimal), "%P")
        self.replace_price_entry.configure(validate="key", validatecommand=vcmd)

        tk.Label(replace_frame, text="New Quantity:").grid(row=0, column=4, padx=5, pady=5)
        self.replace_quantity_entry = tk.Entry(replace_frame)
        self.replace_quantity_entry.grid(row=0, column=5, padx=5)
        vcmd = (self.root.register(self.validate_decimal), "%P")
        self.replace_quantity_entry.configure(validate="key", validatecommand=vcmd)
        tk.Button(replace_frame, text="Replace Selected Order", command=self.replace_order, bg=self.replace_bg).grid(row=0, column=6, padx=5)
        

        cancel_frame = tk.Frame(self.root)
        cancel_frame.pack(pady=5, fill=tk.X, side=tk.TOP)
        tk.Button(cancel_frame, text="Cancel Selected Order", command=self.cancel_order, bg=self.cancel_bg).grid(row=0, column=0, padx=5)
        
    def create_trades_component(self):
        trade_frame = tk.Frame(self.top_frame)
        trade_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)
        
        label = tk.Label(trade_frame, text="Trades")
        label.pack(side=tk.TOP)
        
        columns = ("Quantity", "Price", "Side", "ID")
        trades_tree = ttk.Treeview(trade_frame, columns=columns, show="headings", height=10)
        for col in columns:
            trades_tree.heading(col, text=col)
            trades_tree.column(col, width=60, anchor=tk.CENTER)
        trades_tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        
        trades_tree.tag_configure("buy_light", background=self.light_buy_color)
        trades_tree.tag_configure("buy_bold", background=self.bold_buy_color)
        trades_tree.tag_configure("sell_light", background=self.light_sell_color)
        trades_tree.tag_configure("sell_bold", background=self.bold_sell_color)
        trades_tree.bind("<Button-1>", lambda event: self.block_selection(trades_tree, event))
        
        self.trades_tree = trades_tree
        
    def on_open_order_selected(self, tree, event):
        # columns = ("Price", "Quantity", "Open Quantity", "Side", "ID")
        # Get the selected item
        selected_item = tree.selection()
        if selected_item:
            item = selected_item[0]  # Get the first item (single selection)
            values = tree.item(item, "values")  # Get the values of the selected row
            self.selected_order_id = values[-1]
            self.set_entry_text(self.replace_price_entry, values[0])
            self.set_entry_text(self.replace_quantity_entry, values[2])
        else:
            self.selected_order_id = None
        
    def create_open_orders_component(self):
        open_orders_frame = tk.Frame(self.top_frame)
        open_orders_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)
        
        label = tk.Label(open_orders_frame, text="Open Orders")
        label.pack(side=tk.TOP)
        
        columns = ("Price", "Quantity", "Open Quantity", "Side", "ID")
        open_orders_tree = ttk.Treeview(open_orders_frame, columns=columns, show="headings", height=10, selectmode="browse")
        for col in columns:
            open_orders_tree.heading(col, text=col)
            open_orders_tree.column(col, width=60, anchor=tk.CENTER)
        open_orders_tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        
        open_orders_tree.tag_configure("buy_light", background=self.light_buy_color)
        open_orders_tree.tag_configure("buy_bold", background=self.bold_buy_color)
        open_orders_tree.tag_configure("sell_light", background=self.light_sell_color)
        open_orders_tree.tag_configure("sell_bold", background=self.bold_sell_color)
        open_orders_tree.bind("<<TreeviewSelect>>", lambda event: self.on_open_order_selected(open_orders_tree, event))
        
        self.open_orders_tree = open_orders_tree
        
    def show_error(self, message: str):
        error_window = tk.Toplevel(self.root)
        error_window.title("Error")
        tk.Label(error_window, text=message, fg="red").pack(padx=10, pady=10)
        tk.Button(error_window, text="Close", command=error_window.destroy).pack(pady=5)
        
    def submit_order(self, side: Side):
        try:
            price = Decimal(self.insert_price_entry.get())
            if price < Decimal("0"):
                self.show_error("Price must be greater than 0.")
            qty = Decimal(self.insert_quantity_entry.get())
            if qty < Decimal("0"):
                self.show_error("Quantity must be greater than 0.")
        except Exception:
            self.show_error("Invalid price or quantity!")
            return
        
                # Create and submit order
        order = Order(cl_ord_id=string_helper.generate_uuid(),
                      order_id=string_helper.generate_uuid(),
                      side=side,
                      price=price,
                      qty=qty,
                      symbol="TEST")
        self.open_orders[order.order_id] = order
        self.orderbook.submit_order(order)
        self.refresh_ui()
        
    def replace_order(self):
        if self.selected_order is None:
            self.show_error("No order selected")
            return
        try:
            price = Decimal(self.replace_price_entry.get())
            if price < Decimal("0"):
                self.show_error("Price must be greater than 0.")
            qty = Decimal(self.replace_quantity_entry.get())
            if qty < Decimal("0"):
                self.show_error("Quantity must be greater than 0.")
            qty += self.selected_order.filled_qty
        except Exception:
            self.show_error("Invalid price or quantity!")
            return
        
        self.orderbook.replace_order(self.selected_order, price, qty)
        self.refresh_ui()
        
    def cancel_order(self):
        if self.selected_order_id is None:
            self.show_error("No order selected")
            return
        
        order = self.open_orders[self.selected_order_id]
        self.orderbook.cancel_order(order)
        self.refresh_ui()
        
    def refresh_open_orders(self):
        for item in self.open_orders_tree.get_children():
            self.open_orders_tree.delete(item)
        for order in self.open_orders.values():
            # ("Quantity", "Open Quantity", "Price", "Side", "ID")
            self.open_orders_tree.insert("", tk.END, values=(order.price, order.qty, order.open_qty, "Buy" if order.side == Side.Buy else "Sell", order.order_id))
            
    def refresh_selected_order_id(self):
        if self.selected_order_id is None:
            self.selected_order_id_label.config(text="")
        else:
            self.selected_order_id_label.config(text=self.selected_order_id)

    def refresh_orderbook(self):
        for item in self.buy_tree.get_children():
            self.buy_tree.delete(item)
        for item in self.sell_tree.get_children():
            self.sell_tree.delete(item)
        
        buy_levels: List[PriceLevel] = []
        for order in self.orderbook.in_order_buy_orders():
            if len(buy_levels) == 0 or buy_levels[-1].price != order.price:
                if len(buy_levels) > 0:
                    total_qty = buy_levels[-1].total_qty
                else:
                    total_qty = Decimal("0")
                buy_levels.append(PriceLevel(order.open_qty, total_qty + order.open_qty, order.price))
            else:
                buy_levels[-1].qty += order.open_qty
                buy_levels[-1].total_qty += order.open_qty
        
        sell_levels: List[PriceLevel] = []
        for order in self.orderbook.in_order_sell_orders():
            if len(sell_levels) == 0 or sell_levels[-1].price != order.price:
                if len(sell_levels) > 0:
                    total_qty = sell_levels[-1].total_qty
                else:
                    total_qty = Decimal("0")
                sell_levels.append(PriceLevel(order.open_qty, total_qty + order.open_qty, order.price))
            else:
                sell_levels[-1].qty += order.open_qty
                sell_levels[-1].total_qty += order.open_qty
                
        
        for i in range(len(sell_levels)):
            sell_level = sell_levels[i]
            tag = self.sell_color_alternates[i % 2]
            self.sell_tree.insert("", tk.END, values=(sell_level.price, sell_level.qty, sell_level.total_qty), tags=(tag,))
            
        for i in range(len(buy_levels)):
            buy_level = buy_levels[i]
            tag = self.buy_color_alternates[i % 2]
            self.buy_tree.insert("", tk.END, values=(buy_level.price, buy_level.qty, buy_level.total_qty), tags=(tag,))
            
    def refresh_ui(self):
        self.refresh_open_orders()
        self.refresh_orderbook()
        
    def on_trade(self, trade: Trade):
        color_index = len(self.trades) % 2
        tag = self.sell_color_alternates[color_index] if trade.active_side == Side.Sell else self.buy_color_alternates[color_index]
        self.trades_tree.insert("", 0, values=(trade.qty, trade.price, trade.active_side.name, trade.trade_id), tags=(tag,))
        self.trades.append(trade)
    
    def on_order_update(self, order: Order):
        if not order.is_open:
            if order.order_id == self.selected_order_id:
                self.selected_order_id = None
                
            del self.open_orders[order.order_id]
    
    def on_cancel_reject(self, order: Order, reject_code: RejectCode):
        self.show_error(f"Cancel Reject: {reject_code.name}")
    
    def on_replace_reject(self, order: Order, reject_code: RejectCode):
        self.show_error(f"Replace Reject: {reject_code.name}")

            
    
    

    

   

    # Run the Tkinter event loop
    def start(self):
        self.root.mainloop()


gui = OrderbookGUI()
gui.start()