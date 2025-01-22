# Matching Engine

## Overview

This matching engine is designed for efficient order matching in financial markets. It utilizes a combination of data structures to optimize matching performance and manage price levels and orders. The order book is represented using **Red-Black Trees** as key-value pairs for each side (buy and sell), where the values are **doubly linked lists** combined with **hash maps** for fast cancellation and replacement after finding the price level.

### Features



- **Red-Black Tree for Price Levels:** The engine uses a red-black tree to store and balance price levels, providing efficient searching, insertion, and deletion operations in O(logN) time where N is the count of price levels for one side(buy or sell) on the orderbook.
- **Doubly Linked List with Hash Map for Orders:** Orders at each price level are stored in a doubly linked list to maintain order of execution, while a hash map allows quick lookups for individual orders for replaces and cancels.
- **Efficient Matching:** The engine supports both limit and market orders with quick matching algorithms.
- **Scalable and Fast:** Designed to handle high-frequency trading environments with fast order matching.

## Installation

### Prerequisites

- **Python 3.13.1+**
### Installation Steps

1. Clone the repository:
   ```bash
   git clone https://github.com/barkin-kaplan/python-matching-engine/
   cd matching-engine
2. Create a virtual environment(Optional)
   ```bash
   path/to/your/pyton -m venv .venv
3. Activate virtual environment(Skip if you skipped step 2)
   Windows
   ```bash
   .venv\Scripts\activate
   ```
   Macos
   ```bash
   source .venv/bin/activate
   
4. Install python dependencies
   ```bash
   pip install -r requirements.txt
5. Run orderbookgui.py for UI demo
   ```bash
   python orderbook_gui.py
6. Run performance tests
   ```bash
   python orderbook_perf_test.py
   
### Running Unit Tests
You can run unit tests locally with
```bash
pytest
```
Generate coverage reports
   1. only line coverage
      ```bash
        pytest --cov --cov-report=html
   3. line and branch coverage
      ```bash
        pytest --cov --cov-branch --cov-report=html

