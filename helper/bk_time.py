from datetime import datetime
import time
import pytz

only_micro_format = "%H:%M:%S.%f"
micro_format = "%Y%m%d %H:%M:%S.%f"
seconds_format = "%Y%m%d %H:%M:%S"
only_date_format = "%Y%m%d"
minute_format = "%Y%m%d %H:%M"

xms_format = "%Y-%m-%dT%H:%M:%S.%f%z"

def is_offset_naive(d: datetime):
    return d.tzinfo is None or d.tzinfo.utcoffset(d) is None


def get_current_time_micros() -> int:
    seconds_time = time.time()
    return int(seconds_time * 1000000)

def get_current_time_millis() -> int:
    seconds_time = time.time()
    return int(seconds_time * 1000)

def get_utc_now_micro_formatted() -> str:
    return datetime.utcnow().strftime(micro_format)

def time_micros_to_datetime(time_micros: int) -> datetime:
    return datetime.fromtimestamp(time_micros/1000000, tz=pytz.UTC)

def time_millis_to_datetime(time_millis: int) -> datetime:
    return datetime.fromtimestamp(time_millis/1000, tz=pytz.UTC)

# Input: subdivision in seconds (integer) that divides the minute or the hour exactly.
# Sleeps until the next subdivision.
# e.g.: sync_sleep(10) at 01:14:13 sleeps until 01:14:20,
# e.g.: sync_sleep(1800) at 01:14:13 sleeps until 01:30:00
def sync_sleep(subdivision: float, shift: float = 0, debug: bool = False):
    if not ((subdivision < 60 and 60 % subdivision == 0) or (subdivision >= 60 and subdivision <= 3600 and 3600 % subdivision == 0)):        
        raise Exception(f"Invalid {subdivision} value.")
    
    current = time.time()
    next_aligned_time = ((current // subdivision) + 1) * subdivision + shift
    remaining = next_aligned_time - current
    if remaining < 0:
        next_aligned_time += subdivision
        remaining = next_aligned_time - current
        
    if debug:
        print(f"Going to sleep for {remaining} seconds.")
    time.sleep(remaining)
    # Returns the exact planned wake up time (should be almost equal to time of execution).
    return datetime.fromtimestamp(current + remaining, tz=pytz.UTC)

# aaa = datetime(hour=1, minute=5)
# recurrence = 7200
