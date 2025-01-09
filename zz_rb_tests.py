from decimal import Decimal
import gc
import random
import time
from helper import bk_time
from helper.collections.red_black_tree import RBNode, RedBlackTree


def test_insert():
    rb = RedBlackTree()
    rb[Decimal("0")] = "heyoo245"
    print(list(rb.in_order()))
    rb[Decimal("1")] = "heyoo24"
    print(list(rb.in_order()))
    rb[Decimal("2")] = "heyoo32"
    print(list(rb.in_order()))
    rb[Decimal("1")] = "heyoo26"
    value = rb.insert_or_get(Decimal("1"), "24tsfg")
    assert value == "heyoo26"
    print(list(rb.in_order()))
    del rb[Decimal("0")]
    print(list(rb.in_order()))
    del rb[Decimal("1")]
    print(list(rb.in_order()))
    del rb[Decimal("2")]
    print(list(rb.in_order()))
    
def test_insert_rand():
    rb = RedBlackTree()
    random_nums = []
    expected_duplicates = set()
    count = 10000
    for i in range(count * 2):
        num = Decimal(random.randint(1, count))
        if num in random_nums:
            expected_duplicates.add(num)
        random_nums.append(num)
    
    # print(random_nums)
    for i in range(count * 3):
        num = random.randint(0, len(random_nums) - 1)
        delete_insert = random.randint(1, 2) == 1
        if delete_insert:
            try:
                rb[num] = f"val{num}"
            except DuplicateKeyException as e:
                pass
        else:
            rb.__delete(num)
        rb_count = RBNode.rb_count
        item_count = len(list(rb.__inorder()))
        print(f"rb count: {rb_count}, item count: {item_count}")
        if item_count > 0:
            if abs((rb_count / item_count) - 1) > 0.1:
                a = 5
        
    print(f"len nums: {len(set(random_nums))}")
    
def test_performance():
    rand_nums = []
    count = 10000
    for i in range(count):
        num = random.randint(1, count)
        if num not in rand_nums:
            rand_nums.append(num)
    l = []
    d = dict()
    rb = RedBlackTree()
    start = bk_time.get_current_time_micros()
    for num in rand_nums:
        l.append(num)
    end = bk_time.get_current_time_micros()
    print(f"list insert: {end - start}, avg: {(end - start) / len(rand_nums)}")
    
    start = bk_time.get_current_time_micros()
    for num in rand_nums:
        rb[num] = num
    end = bk_time.get_current_time_micros()
    print(f"rb insert: {end - start}, avg: {(end - start) / len(rand_nums)}")
    
    start = bk_time.get_current_time_micros()
    for num in rand_nums:
        d[num] = num
    end = bk_time.get_current_time_micros()
    print(f"dict insert: {end - start}, avg: {(end - start) / len(rand_nums)}")
    
    start = bk_time.get_current_time_micros()
    for num in rand_nums:
        a = l.index(num)
    end = bk_time.get_current_time_micros()
    print(f"list get: {end - start}, avg: {(end - start) / len(rand_nums)}")
    
    start = bk_time.get_current_time_micros()
    for num in rand_nums:
        a = rb[num]
    end = bk_time.get_current_time_micros()
    print(f"rb get: {end - start}, avg: {(end - start) / len(rand_nums)}")
    
    start = bk_time.get_current_time_micros()
    for num in rand_nums:
        a = d[num]
    end = bk_time.get_current_time_micros()
    print(f"dict get: {end - start}, avg: {(end - start) / len(rand_nums)}")
    
    start = bk_time.get_current_time_micros()
    for num in rand_nums:
        l.remove(num)
    end = bk_time.get_current_time_micros()
    print(f"list delete: {end - start}, avg: {(end - start) / len(rand_nums)}")
    
    start = bk_time.get_current_time_micros()
    for num in rand_nums:
        del rb[num]
    end = bk_time.get_current_time_micros()
    print(f"rb delete: {end - start}, avg: {(end - start) / len(rand_nums)}")
    
    start = bk_time.get_current_time_micros()
    for num in rand_nums:
        del d[num]
    end = bk_time.get_current_time_micros()
    print(f"dict delete: {end - start}, avg: {(end - start) / len(rand_nums)}")
    
def test_insert_or_get():
    rb1 = RedBlackTree()
    rb2 = RedBlackTree()
    rand_nums = []
    count = 10000
    for i in range(count):
        num = random.randint(1, count)
        rand_nums.append(num)
            
    start = bk_time.get_current_time_micros()
    for num in rand_nums:
        result = rb1[num]
        if result is None:
            rb1[num] = num
    end = bk_time.get_current_time_micros()
    print(f"search + insert : {end - start}, avg: {(end - start) / len(rand_nums)}")
    
    start = bk_time.get_current_time_micros()
    for num in rand_nums:
        result = rb2.insert_or_get(num, num)
        
    end = bk_time.get_current_time_micros()
    print(f"insert or get : {end - start}, avg: {(end - start) / len(rand_nums)}")
        
    
test_insert_or_get()
