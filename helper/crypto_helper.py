import hashlib
import random
import uuid

_random_string_pool = ""


for i in range(26):
    _random_string_pool += chr(ord('a') + i)
    
for i in range(26):
    _random_string_pool += chr(ord('A') + i)
    
for i in range(10):
    _random_string_pool += chr(ord('0') + i)
    
_l = len(_random_string_pool) - 1


def get_sha512_hash_hex(s: str) -> str:
    sha512 = hashlib.sha512()
    sha512.update(s.encode('utf-8'))
    hash_hex = sha512.hexdigest()
    return hash_hex


def generate_uuid() -> str:
    return str(uuid.uuid4())

def generate_random_string(length: int, pool = _random_string_pool):
    s = ""
    for i in range(length):
        s += _random_string_pool[random.randint(0, _l)]
        
    return s
    
