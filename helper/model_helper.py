

from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Union
# Recursive to_dict function, useful for serializing objects for mongo.
# TODO: Might be useful to add a "hasattr" for "to_mongo" since classes may have specialized mongo serialization (with extra parameter for "top level call" indicating not to call to_mongo causing a loop).
def recursive_to_dict(obj, classkey=None) -> Union[Dict, Any]:
    if isinstance(obj, dict):
        data = {}
        for (k, v) in obj.items():
            data[k] = recursive_to_dict(v, classkey)
        return data
    elif isinstance(obj, Enum):
        return obj.value
    elif hasattr(obj, "_ast"):
        return recursive_to_dict(obj._ast())
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [recursive_to_dict(v, classkey) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict([(key, recursive_to_dict(value, classkey)) 
            for key, value in obj.__dict__.items() 
            if not callable(value) and not key.startswith('_')])
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj

def decimals_8(n):
    return "{:.8f}".format(n)

class BisectWrapper:
    def __init__(self, iterable, key):
        self.it = iterable
        self.key = key

    def __getitem__(self, i):
        return self.key(self.it[i])

    def __len__(self):
        return len(self.it)