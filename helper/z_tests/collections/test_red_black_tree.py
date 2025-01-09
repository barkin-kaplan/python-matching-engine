

from decimal import Decimal
import math
import random
from typing import Optional
from helper.collections.red_black_tree import RBNode, RedBlackTree


def test_height():
    
    rb = RedBlackTree()
    def min_height(sub_root: Optional[RBNode]) -> int:
        if sub_root is None or (sub_root.left is None and sub_root.right is None):
            return 1
        
        return min(min_height(sub_root.left) + 1, min_height(sub_root.right) + 1)
    def max_height(sub_root: Optional[RBNode]) -> int:
        if sub_root is None or (sub_root.left is None and sub_root.right is None):
            return 1
        
        return max(max_height(sub_root.left) + 1, max_height(sub_root.right) + 1)
    count = 1000
    for i in range(count):
        rb[i] = i
        max_h = max_height(rb.root)
        min_h = min_height(rb.root)
        assert min_h * 2 >= max_h
        assert (math.log(i + 1, 2) + 1) * 2 >= max_h
        

def test_insert():
    rb = RedBlackTree()
    random_nums = list()
    count = 100
    for i in range(count * 2):
        num = Decimal(random.randint(1, count))
        random_nums.append(num)
        
    for num in random_nums:
        rb[num] = f"val{num}"
            
    assert len(list(rb.in_order())) == len(set(random_nums))
            
        
    