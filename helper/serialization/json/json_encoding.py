import sys
from decimal import Decimal
from datetime import datetime
from enum import Enum
from typing import Optional

from helper import bk_decimal

if "pypy" in sys.executable:
    import json
else:
    import orjson



def orjson_encode(obj):
    return str(orjson.dumps(obj, default=__encode), "utf-8")


def json_encode(obj):
    return json.dumps(obj, default=__encode)


def orjson_decode(json_s: str):
    return orjson.loads(json_s)


def json_decode(json_s: str):
    return json.loads(json_s)


if "pypy" in sys.executable:
    my_encode = json_encode
    my_decode = json_decode
else:
    my_encode = orjson_encode
    my_decode = orjson_decode


def __encode(obj):
    to_be_serialized = None
    if isinstance(obj, dict):
        to_be_serialized = obj
    elif isinstance(obj, set):
        to_be_serialized = list(obj)
    elif isinstance(obj, datetime):
        to_be_serialized = obj.strftime("%Y/%m/%d, %H:%M:%S")
    elif isinstance(obj, Decimal):
        to_be_serialized = str(obj)
    elif issubclass(type(obj), Enum):
        to_be_serialized = obj.value
    else:
        to_be_serialized = obj.__dict__

    return to_be_serialized


def encode(obj) -> str:
    return my_encode(obj)


def decode(json_s: str):
    return my_decode(json_s)


def convert_or_none(value, convert):
    return convert(value) if value is not None else None

def decimal_to_json(d: Optional[Decimal], round_precision: Optional[int] = None):
    if d is None:
        return None

    if round_precision is not None:
        d = bk_decimal.round_decimal(d, round_precision)
    formatted = format(d, "f")
    if "." in formatted:
        return formatted.rstrip('0').rstrip('.')

    return formatted


