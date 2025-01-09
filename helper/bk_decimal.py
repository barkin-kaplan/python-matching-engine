from abc import ABC
from dataclasses import dataclass
import datetime
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from enum import Enum
import math
from statistics import mean
import time
from typing import Any, Callable, Generator, List, Optional, Tuple, TypeVar, Union

from helper import bk_time
from helper.collections.list_extension import merge_in_order

epsilon = Decimal("0.0000000001")
T = TypeVar("T")

@dataclass
class TimeValue:
    timestamp: float
    value: Decimal
    
    def __repr__(self):
        return f"{datetime.datetime.fromtimestamp(self.timestamp).strftime(bk_time.seconds_format)} \t {self.value}"

def decimal_arange(start, stop, step):
    current = start
    while current < stop:
        yield current
        current += step

def decimal_to_float(val):
    if isinstance(val, Decimal):
        return float(val)
    else:
        return val

def decimal_without_redundant_zeroes(s: Union[str, Decimal]):
    if isinstance(s, Decimal):
        s = str(s)
    s = s.rstrip('0').rstrip('.') if '.' in s else s
    return Decimal(s)


def is_epsilon_equal(d1: Decimal, d2: Decimal, epsilon: Decimal = epsilon):
    diff = d1 - d2
    return abs(diff) < epsilon

def epsilon_lt(d1: Decimal, d2: Decimal, epsilon: Decimal = epsilon):
    return d1 < d2 and not is_epsilon_equal(d1, d2, epsilon)

def epsilon_lte(d1: Decimal, d2: Decimal, epsilon: Decimal = epsilon):
    return d1 <= d2 or is_epsilon_equal(d1, d2, epsilon)

def epsilon_gt(d1: Decimal, d2: Decimal, epsilon: Decimal = epsilon):
    return d1 > d2 and not is_epsilon_equal(d1, d2, epsilon)

def epsilon_gte(d1: Decimal, d2: Decimal, epsilon: Decimal = epsilon):
    return d1 >= d2 or is_epsilon_equal(d1, d2, epsilon)

# usage: floor_decimal(Decimal("0.6324"), 2)
def floor_decimal(value, precision):
    if precision >= 0:
        return value.quantize(Decimal('0.' + '0' * (precision - 1) + '1'), rounding=ROUND_DOWN)
    else:
        rounding_factor = Decimal('1' + '0' * abs(precision))
        return (value / rounding_factor).quantize(Decimal('1'), rounding=ROUND_DOWN) * rounding_factor

def round_decimal(value, precision):
    if precision >= 1:
        return value.quantize(Decimal('0.' + '0' * (precision - 1) + '1'), rounding=ROUND_HALF_UP)
    else:
        rounding_factor = Decimal('1' + '0' * abs(precision))
        return (value / rounding_factor).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * rounding_factor
    
def fibonacci_generator(start_index, end_index) -> Generator[Decimal, None, None]:
    a, b = Decimal("0"), Decimal("1")
    current_index = 0
    while current_index < end_index:
        if current_index >= start_index:
            yield a
        a, b = b, a + b
        current_index += 1

def fibonacci_slices(number: Decimal, fibonacci_count: int, quantize: Optional[Decimal] = None,  minimum: Optional[Decimal] = None, fibonacci_transformer=lambda x: x) -> List[Decimal]:
    fib_sequence = list(fibonacci_generator(1, 1 + fibonacci_count))
    for i in range(len(fib_sequence)):
        fib_sequence[i] = fibonacci_transformer(fib_sequence[i])
    fib_total = sum(fib_sequence)
    
    slices = []
    for fib in fib_sequence:
        ratio = Decimal(str(fib / fib_total))
        slice = number * ratio
        if minimum is not None and slice < minimum:
            slice = minimum
        if quantize is not None:
            slices.append(slice.quantize(quantize))
        else:
            slices.append(slice)

    return slices



def get_decimal_places(decimal_number):
    # Convert to Decimal if it's not already
    if not isinstance(decimal_number, Decimal):
        decimal_number = Decimal(decimal_number)
    
    # Use the as_tuple method
    decimal_tuple = decimal_number.as_tuple()
    # The exponent indicates the number of decimal places, which is negative
    decimal_places = -decimal_tuple.exponent
    
    return decimal_places

class ExtremumType(Enum):
    Min = 0
    Max = 1
    Single = 2

def find_extrema(extremum_locality_seconds: float, data: List[TimeValue]) -> List[Tuple[int, ExtremumType]]:
    if len(data) == 0:
        return []
    maximums: List[Tuple[int, ExtremumType]] = []
    minimums: List[Tuple[int, ExtremumType]] = []
    
    max_iter_index = 0
    while max_iter_index < len(data):
        center = data[max_iter_index]
        # traverse backwards
        back_iter = max_iter_index - 1
        is_max = True
        while back_iter > -1 and is_max:
            back = data[back_iter]
            if back.timestamp < center.timestamp - extremum_locality_seconds:
                break
            if back.value > center.value:
                is_max = False
                break
                
            back_iter -= 1
            
        front_iter = max_iter_index + 1
        while front_iter < len(data) and is_max:
            front = data[front_iter]
            if front.timestamp > center.timestamp + extremum_locality_seconds:
                break
            if front.value > center.value:
                is_max = False
                break
                
            front_iter += 1
                
        if is_max:
            # if not len(maximums) > 0 or not is_epsilon_equal(data[maximums[-1][0]].value, data[max_iter_index].value):
            maximums.append((max_iter_index, ExtremumType.Max))
            
        max_iter_index = front_iter
        
    min_iter_index = 0
    while min_iter_index < len(data):
        center = data[min_iter_index]
        # traverse backwards
        back_iter = min_iter_index - 1
        is_min = True
        while back_iter > -1 and is_min:
            back = data[back_iter]
            if back.timestamp < center.timestamp - extremum_locality_seconds:
                break
            if back.value < center.value:
                is_min = False
                break
                
            back_iter -= 1
            
        front_iter = min_iter_index + 1
        while front_iter < len(data) and is_min:
            front = data[front_iter]
            if front.timestamp > center.timestamp + extremum_locality_seconds:
                break
            if front.value < center.value:
                is_min = False
                break
                
            front_iter += 1
                
        if is_min:
            # if not len(minimums) > 0 or not is_epsilon_equal(data[minimums[-1][0]].value, data[min_iter_index].value):
            minimums.append((min_iter_index, ExtremumType.Min))
            
        min_iter_index = front_iter
                
    merged = merge_in_order(maximums, minimums, lambda x1, x2: x1[0] < x2[0])
    i = 0
    while i < len(merged) - 1:
        if merged[i][0] == merged[i + 1][0]:
            merged[i] = (i, ExtremumType.Single)
            merged = merged[:i + 1] + merged[i + 2:]
        else:
            i += 1
    # for item in merged:
    #     print(f"{data[item[0]]}-{item[1]}\t", end="")
    # print()
    return merged
    
class TimeValueCalculator(ABC):
    def __init__(self, period_secs: int):
        self.period_secs = period_secs
        self._values: List[TimeValue] = []
        
    def add_data(self, time_value: TimeValue):
        self._values.append(time_value)

class TimeWeightedAverage(TimeValueCalculator):
    def __init__(self, period_secs: int):
        super().__init__(period_secs)
        
    def calculate(self) -> Optional[Decimal]:
        
        if len(self._values) == 0:
            return None
        now = time.time()
        time_range_start = now - self.period_secs
        to_be_removed_index = -1
        for i in range(len(self._values)):
            value = self._values[i]
            if value.timestamp < time_range_start:
                to_be_removed_index = i
            else:
                break
        to_be_removed_index -= 1
        if to_be_removed_index > -1:
            self._values = self._values[to_be_removed_index:]

        time_diffs = []
        if time_range_start > self._values[0].timestamp:
            if len(self._values) > 1:
                time_diffs.append(self._values[1].timestamp - time_range_start)
            else:
                return self._values[0].value

        index_start = 2 if len(time_diffs) > 0 else 1
        for i in range(index_start, len(self._values)):
            time_diffs.append(self._values[i].timestamp - self._values[i - 1].timestamp)
        time_diffs.append(now - self._values[-1].timestamp)
        total_time = sum(time_diffs)

        weighted_avg = sum(self._values[i].value * Decimal(str(time_diffs[i] / total_time)) for i in range(len(time_diffs)))
        return Decimal(str(weighted_avg))
    
class TimeWeightedDirectionalVolatility(TimeValueCalculator):
    @dataclass
    class _Change:
        duration: float
        timestamp: float
        diff: Decimal
        
    def __init__(self, period_secs: int, extremum_locality: int, is_recent_weights_higher: bool = True):
        super().__init__(period_secs)
        self.extremum_locality = extremum_locality
        if is_recent_weights_higher:
            self.calculate = self._calculate_recent_weights_higher
        else:
            self.calculate = self._calculate_equal_weights
        
    def _trim_values_and_calculate_diffs(self, now: float) -> List['TimeWeightedDirectionalVolatility._Change']:
        time_range_start = now - self.period_secs
        to_be_removed_index = -1
        for i in range(len(self._values)):
            value = self._values[i]
            if value.timestamp < time_range_start:
                to_be_removed_index = i
            else:
                break
        to_be_removed_index += 1
        if to_be_removed_index > -1:
            self._values = self._values[to_be_removed_index:]
            
        if len(self._values) == 0:
            return []
            
        extrema = find_extrema(self.extremum_locality, self._values)
        if len(extrema) == 0:
            return []
        # duration, difference
        diffs: List[TimeWeightedDirectionalVolatility._Change] = []
        for i in range(1, len(extrema)):
            prev_extremum_index = extrema[i - 1][0]
            current_extremum_index = extrema[i][0]
            prev_value = self._values[prev_extremum_index]
            current_value = self._values[current_extremum_index]
            diff = (current_value.value - prev_value.value) / prev_value.value
            duration = current_value.timestamp - prev_value.timestamp
            timestamp = prev_value.timestamp
            diffs.append(TimeWeightedDirectionalVolatility._Change(duration, timestamp, diff))
        
        # if last extremum point is not the last point of data then add last data point to diffs list
        # if extrema[-1][0] != len(self._values) - 1:
        #     last_extremum = self._values[extrema[-1][0]]
        #     last_data_point = self._values[-1]
        #     diff = (last_data_point.value - last_extremum.value) / last_extremum.value
        #     duration = last_data_point.timestamp - last_extremum.timestamp
        #     timestamp = last_extremum.timestamp
        #     diffs.append(TimeWeightedDirectionalVolatility._Change(duration, timestamp, diff))
        
        return diffs
        
    def _calculate_recent_weights_higher(self, now: Optional[float] = None) -> Optional[Decimal]:
        if len(self._values) == 0:
            return None
        
        if now is None:
            now = time.time()
            
        diffs = self._trim_values_and_calculate_diffs(now)
        if len(diffs) == 0:
            return None
        
        start_time = self._values[0].timestamp
        weight_total = Decimal("0")
        value_total = Decimal("0")
        for diff in diffs:
            # ts......t1........t2
            # sqrt(t1 - ts), sqrt(t2 - ts)
            time_distance_weight = ((diff.timestamp - start_time + 1) / (now - start_time)) ** 0.5
            # add 1 because we dont want values close to 1 as it is an identity element for multiplication
            time_distance_weight += 1
            time_duration_weight = diff.duration
            weight = Decimal(time_distance_weight * time_duration_weight)
            weight_total += weight
            value_total += weight * diff.diff
            
        return Decimal(value_total / weight_total)
    
    def _calculate_equal_weights(self, now: Optional[float] = None) -> Optional[Decimal]:
        if len(self._values) == 0:
            return None
        
        if now is None:
            now = time.time()
            
        diffs = self._trim_values_and_calculate_diffs(now)
        if len(diffs) == 0:
            return None
        
        weight_total = 0
        value_total = 0
        for diff in diffs:
            time_duration_weight = Decimal(diff.duration)
            weight_total += time_duration_weight
            value_total += time_duration_weight * diff.diff
            
        return Decimal(value_total / weight_total)
    
        
    
class TimeBasedVolatility(TimeValueCalculator):
    def __init__(self, period_secs: int, lower_period_secs: int):
        super().__init__(period_secs)
        self.lower_period_secs = lower_period_secs
        self._values: List[TimeValue] = []
        
    def add_data(self, value: TimeValue):
        self._values.append(value)
        
    def calculate(self, now: Optional[float] = None) -> List[Tuple[datetime.datetime, Decimal]]:
        if len(self._values) == 0:
            return []
        
        if now is None:
            now = time.time()
        time_range_start = now - self.period_secs
        to_be_removed_index = -1
        for i in range(len(self._values)):
            value = self._values[i]
            if value.timestamp < time_range_start:
                to_be_removed_index = i
            else:
                break
        to_be_removed_index += 1
        if to_be_removed_index > 0:
            self._values = self._values[to_be_removed_index:]

        anchor_timestamp = self._values[0].timestamp
        iter_chunk = []
        chunks: List[Tuple[float, List[TimeValue]]] = []
        for value in self._values:
            if value.timestamp >= anchor_timestamp + self.lower_period_secs:
                if len(iter_chunk) > 0:
                    chunks.append((anchor_timestamp, iter_chunk))
                iter_chunk = []
                anchor_timestamp += self.lower_period_secs
                
            iter_chunk.append(value)
            
        if len(iter_chunk) > 0:
            chunks.append((anchor_timestamp, iter_chunk))
            
        chunk_volatilities: List[Tuple[datetime.datetime, Decimal]] = []
        for timestamp, chunk in chunks:
            if len(chunk) == 0:
                continue
            max_value = chunk[0]
            min_value = chunk[0]
            for value in chunk:
                if value.value > max_value.value:
                    max_value = value
                elif value.value < min_value.value:
                    min_value = value
            
            date = datetime.datetime.fromtimestamp(timestamp)
            chunk_volatilities.append((date, (max_value.value - min_value.value) / min_value.value))
        
        return chunk_volatilities
