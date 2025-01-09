import datetime
from typing import Any, Callable, List, Tuple, TypeVar

T = TypeVar("T")

def resize(l: List, new_count: int):
    if len(l) == new_count: return
    elif len(l) < new_count:
        for i in range(new_count - len(l)):
            l.append(None)
    else:
        for i in range(len(l) - new_count):
            l.pop(-1)


def clamp_fit_inside_list(l, index) -> int:
    length = len(l)
    index = index if index < length else length -1
    index = index if index > -1 else 0
    return index


def binary_search(arr: List[T], x, low=None, high=None, keyfunc: Callable[[T], Any]=lambda v: v, reverse=False) -> Tuple[int, bool]:
    """
    Searches for an element in the list and returns the index if the element is found, if not the correct index is returned with a boolean value of False
    :param arr:
    :param x:
    :param low:
    :param high:
    :param keyfunc:
    :param reverse:
    :return:
    """
    if low is None:
        low = 0
    if high is None:
        high = len(arr) - 1
    # Check base case
    if high >= low:
        mid = (high + low) // 2

        # If element is present at the middle itself
        mid_value = keyfunc(arr[mid])
        # convert key if needed
        if isinstance(x, type(arr[mid])):
            x_value = keyfunc(x)
        else:
            x_value = x
        if mid_value == x_value:
            return mid, True

        # If element is smaller than mid, then it can only
        # be present in left subarray
        elif mid_value > x_value:
            if reverse:
                return binary_search(arr, x, mid + 1, high, keyfunc, reverse)
            else:
                return binary_search(arr, x, low, mid - 1, keyfunc, reverse)

        # Else the element can only be present in right subarray
        else:
            if reverse:
                return binary_search(arr, x, low, mid - 1, keyfunc, reverse)
            else:
                return binary_search(arr, x, mid + 1, high, keyfunc, reverse)

    else:
        # Element is not present in the array
        return low, False


def merge_in_order(l1: List, l2: List, comparison=lambda v1,v2: v1 < v2):
    """
    Merges two lists with given comparison. If comparison lambda is not given merges two lists directly comparing elements
    :param l1:
    :param l2:
    :param comparison:
    :return:
    """
    it1 = 0
    it2 = 0
    length1 = len(l1)
    length2 = len(l2)
    merged = []
    while it1 < length1 and it2 < length2:
        item1 = l1[it1]
        item2 = l2[it2]
        if comparison(item1, item2):
            it1 += 1
            merged.append(item1)
        else:
            it2 += 1
            merged.append(item2)

    while it1 < length1:
        merged.append(l1[it1])
        it1 += 1
    while it2 < length2:
        merged.append(l2[it2])
        it2 += 1

    return merged


def merge_in_order_advanced(info_list):
    """
    Merges n lists with given info list. Sample info list structure is:\n
    info1 = {"data": arr1, "lambda": lambda o: o}\n
    info2 = {"data": arr2, "lambda": lambda o: int(o)}\n
    info3 = {"data": arr3, "lambda": lambda o: int(o)}\n
    :param info_list:
    :return:
    """
    for info in info_list:
        info["iter"] = 0
    info_list = list(filter(lambda o: len(o["data"]) > 0, info_list))
    merged = []
    while len(info_list) > 0:
        next_info = min(info_list, key=lambda i1: i1["lambda"](i1["data"][i1["iter"]]))
        merged.append(next_info["data"][next_info["iter"]])
        next_info["iter"] += 1
        if next_info["iter"] == len(next_info["data"]):
            info_list.remove(next_info)

    return merged




if __name__ == "__main__":
    # Test array
    arr = [("aeyo", 2), ("beyoo", 3), ("cjgs", 4), ("tdgsg", 10), ("zsg", 40)]
    arr_reversed = [("zsg", 40),  ("tdgsg", 10), ("cjgs", 4), ("beyoo", 3), ("aeyo", 2)]
    x = ("caeyoo", 37)

    # Function call
    result = binary_search(arr_reversed, x, keyfunc=lambda v : v[1], reverse=True)

    if result != -1:
        print("Element is present at index", str(result))
    else:
        print("Element is not present in array")

    ######### merge test
    arr1 = sorted([4, 6, 458, 24])
    arr2 = sorted(["534", "34", "8735", "2"], key=lambda v: int(v))
    print(arr1)
    print(arr2)
    result = merge_in_order(arr1, arr2, lambda v1, v2: v1 < int(v2))
    print(result)

    ######merge in order advanced
    arr3 = sorted([34, 5, 6, 3, 1])
    info1 = {"data": arr1, "lambda": lambda o: o}
    info2 = {"data": arr2, "lambda": lambda o: int(o)}
    info3 = {"data": arr3, "lambda": lambda o: int(o)}
    info_list = [info1, info2, info3]
    result = merge_in_order_advanced(info_list)
    print(result)

    print(min(datetime.datetime(2022,8,12,10,34), datetime.datetime(2000,5,3)))
    merge_in_order_advanced()