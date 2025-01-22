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
## Performance Test Results
Firstly running times will be higher than actual repoted execution times because for each test a fresh orderbook is populated beforehand and that takes extra time.
Here are the performance result on my machine. I'm running on a AMD Ryzen 9 7900X3D 12-Core Processor
Insert small (count: 1000) on small price range([1,1000]) took 0.009636402130126953 seconds
Insert small (count: 1000) on medium price range([1,10000]) took 0.00838470458984375 seconds
Insert small (count: 1000) on large price range([1,100000]) took 0.009348392486572266 seconds
Insert medium (count: 10000) on small price range([1,1000]) took 0.11107563972473145 seconds
Insert medium (count: 10000) on medium price range([1,10000]) took 0.16815590858459473 seconds
Insert medium (count: 10000) on large price range([1,100000]) took 0.19087958335876465 seconds
Insert large (count: 100000) on small price range([1,1000]) took 1.2290959358215332 seconds
Insert large (count: 100000) on medium price range([1,10000]) took 4.860355377197266 seconds
Insert large (count: 100000) on large price range([1,100000]) took 11.840525150299072 seconds
Replace small(count: 1000) for small order count(1000), for small price range (([1,1000])) took 0.005799293518066406 seconds
Replace small(count: 1000) for small order count(1000), for medium price range (([1,10000])) took 0.006266117095947266 seconds
Replace small(count: 1000) for small order count(1000), for large price range (([1,100000])) took 0.0076313018798828125 seconds
Replace small(count: 1000) for medium order count(10000), for small price range (([1,1000])) took 0.00503993034362793 seconds
Replace small(count: 1000) for medium order count(10000), for medium price range (([1,10000])) took 0.0064830780029296875 seconds
Replace small(count: 1000) for medium order count(10000), for large price range (([1,100000])) took 0.008660554885864258 seconds
Replace small(count: 1000) for large order count(100000), for small price range (([1,1000])) took 0.005796909332275391 seconds
Replace small(count: 1000) for large order count(100000), for medium price range (([1,10000])) took 0.006762027740478516 seconds
Replace small(count: 1000) for large order count(100000), for large price range (([1,100000])) took 0.007879972457885742 seconds
Replace medium(count: 10000) for small order count(1000), for small price range (([1,1000])) took 0.030429840087890625 seconds
Replace medium(count: 10000) for small order count(1000), for medium price range (([1,10000])) took 0.04793953895568848 seconds
Replace medium(count: 10000) for small order count(1000), for large price range (([1,100000])) took 0.04391288757324219 seconds
Replace medium(count: 10000) for medium order count(10000), for small price range (([1,1000])) took 0.045610904693603516 seconds
Replace medium(count: 10000) for medium order count(10000), for medium price range (([1,10000])) took 0.26291775703430176 seconds
Replace medium(count: 10000) for medium order count(10000), for large price range (([1,100000])) took 0.40394067764282227 seconds
Replace medium(count: 10000) for large order count(100000), for small price range (([1,1000])) took 0.05797266960144043 seconds
Replace medium(count: 10000) for large order count(100000), for medium price range (([1,10000])) took 0.06719279289245605 seconds
Replace medium(count: 10000) for large order count(100000), for large price range (([1,100000])) took 0.3151721954345703 seconds
Replace large(count: 100000) for small order count(1000), for small price range (([1,1000])) took 0.10948491096496582 seconds
Replace large(count: 100000) for small order count(1000), for medium price range (([1,10000])) took 0.12932062149047852 seconds
Replace large(count: 100000) for small order count(1000), for large price range (([1,100000])) took 0.14337611198425293 seconds
Replace large(count: 100000) for medium order count(10000), for small price range (([1,1000])) took 0.2379775047302246 seconds
Replace large(count: 100000) for medium order count(10000), for medium price range (([1,10000])) took 1.489325761795044 seconds
Replace large(count: 100000) for medium order count(10000), for large price range (([1,100000])) took 3.119692802429199 seconds
Replace large(count: 100000) for large order count(100000), for small price range (([1,1000])) took 0.5060892105102539 seconds
Replace large(count: 100000) for large order count(100000), for medium price range (([1,10000])) took 0.9982717037200928 seconds
Replace large(count: 100000) for large order count(100000), for large price range (([1,100000])) took 28.259878635406494 seconds
Cancel small(count: 1000) for small order count(1000), for small price range (([1,1000])) took 0.001171112060546875 seconds
Cancel small(count: 1000) for small order count(1000), for medium price range (([1,10000])) took 0.0010793209075927734 seconds
Cancel small(count: 1000) for small order count(1000), for large price range (([1,100000])) took 0.0010521411895751953 seconds
Cancel small(count: 1000) for medium order count(10000), for small price range (([1,1000])) took 0.0011665821075439453 seconds
Cancel small(count: 1000) for medium order count(10000), for medium price range (([1,10000])) took 0.0017273426055908203 seconds
Cancel small(count: 1000) for medium order count(10000), for large price range (([1,100000])) took 0.0014235973358154297 seconds
Cancel small(count: 1000) for large order count(100000), for small price range (([1,1000])) took 0.0018687248229980469 seconds
Cancel small(count: 1000) for large order count(100000), for medium price range (([1,10000])) took 0.0030813217163085938 seconds
Cancel small(count: 1000) for large order count(100000), for large price range (([1,100000])) took 0.0030138492584228516 seconds
Cancel medium(count: 10000) for small order count(1000), for small price range (([1,1000])) took 0.009098052978515625 seconds
Cancel medium(count: 10000) for small order count(1000), for medium price range (([1,10000])) took 0.009633779525756836 seconds
Cancel medium(count: 10000) for small order count(1000), for large price range (([1,100000])) took 0.009610891342163086 seconds
Cancel medium(count: 10000) for medium order count(10000), for small price range (([1,1000])) took 0.01174473762512207 seconds
Cancel medium(count: 10000) for medium order count(10000), for medium price range (([1,10000])) took 0.013224601745605469 seconds
Cancel medium(count: 10000) for medium order count(10000), for large price range (([1,100000])) took 0.01389455795288086 seconds
Cancel medium(count: 10000) for large order count(100000), for small price range (([1,1000])) took 0.016527891159057617 seconds
Cancel medium(count: 10000) for large order count(100000), for medium price range (([1,10000])) took 0.020753860473632812 seconds
Cancel medium(count: 10000) for large order count(100000), for large price range (([1,100000])) took 0.02716851234436035 seconds
Cancel large(count: 100000) for small order count(1000), for small price range (([1,1000])) took 0.08701229095458984 seconds
Cancel large(count: 100000) for small order count(1000), for medium price range (([1,10000])) took 0.09422993659973145 seconds
Cancel large(count: 100000) for small order count(1000), for large price range (([1,100000])) took 0.09547138214111328 seconds
Cancel large(count: 100000) for medium order count(10000), for small price range (([1,1000])) took 0.10396933555603027 seconds
Cancel large(count: 100000) for medium order count(10000), for medium price range (([1,10000])) took 0.14526009559631348 seconds
Cancel large(count: 100000) for medium order count(10000), for large price range (([1,100000])) took 0.1372520923614502 seconds
Cancel large(count: 100000) for large order count(100000), for small price range (([1,1000])) took 0.14864063262939453 seconds
Cancel large(count: 100000) for large order count(100000), for medium price range (([1,10000])) took 0.19442105293273926 seconds
Cancel large(count: 100000) for large order count(100000), for large price range (([1,100000])) took 0.24105191230773926 seconds


