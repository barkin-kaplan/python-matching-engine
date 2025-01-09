
from typing import List

def contains_sort(string_list: List[str], containers_first=True):
    sorted_list = sorted(string_list, key=lambda x: (len(x), x), reverse=containers_first)
    return sorted(sorted_list, key=lambda x: any(set(x).issubset(set(s)) for s in sorted_list))

if __name__ == "__main__":
    string_list = ["app", "na", "ana", "nana", "a", "apple", "banana", "ban"] 
    sorted_list = contains_sort(string_list) # ['banana', 'apple', 'nana', 'ban', 'app', 'ana', 'na', 'a']
    print(sorted_list)