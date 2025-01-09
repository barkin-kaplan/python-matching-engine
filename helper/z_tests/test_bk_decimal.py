from decimal import Decimal
import time
from typing import List

from helper.bk_decimal import ExtremumType, TimeValue, find_extrema


def test_find_extrema_1():
    # Create sample data
    base_time = time.time()
    data = [TimeValue(base_time + i, (i % 10) * (-1) ** (i // 10)) for i in range(100)]
    
    # Define extremum locality in seconds
    extremum_locality_seconds = 5.0
    
    # Call the function with sample data
    result = find_extrema(extremum_locality_seconds, data)
    
    # Define expected result (this should be calculated or verified manually)
    expected_result = [
        # Add expected result tuples of indices and ExtremumType here
        # For example:
        (0, ExtremumType.Min),
        (9, ExtremumType.Max),
        (19, ExtremumType.Min),
        (29, ExtremumType.Max),
        (39, ExtremumType.Min),
        (49, ExtremumType.Max),
        (59, ExtremumType.Min),
        (69, ExtremumType.Max),
        (79, ExtremumType.Min),
        (89, ExtremumType.Max),
        (99, ExtremumType.Min)
    ]
    
    # Assert the result is as expected
    assert result == expected_result, f"Expected {expected_result} but got {result}"
    
def test_extrema_2():
    base_time = time.time()
    data = [TimeValue(base_time + i, Decimal(str(i % 2))) for i in range(100)]
    for extremum_locality_seconds in range(1, 100):
        result = find_extrema(extremum_locality_seconds, data)
        
        expected_result = []
        
        for i in range(0, 100, extremum_locality_seconds + 1 if extremum_locality_seconds % 2 == 1 else extremum_locality_seconds + 2):
            expected_result.append((i, ExtremumType.Min))
            expected_result.append((i + 1, ExtremumType.Max))
        
        assert result == expected_result, f"Expected {expected_result} but got {result} with extremum locality: {extremum_locality_seconds}"
        
def test_extrema_3():
    data: List[TimeValue] = []
    data.append(TimeValue(0, Decimal("13.20")))
    data.append(TimeValue(0.3, Decimal("12.45")))
    data.append(TimeValue(0.5, Decimal("13.37")))
    data.append(TimeValue(1, Decimal("13.37")))
    
    result = find_extrema(5, data)
    
    assert result == [(1,ExtremumType.Min), 
                      (2, ExtremumType.Max)]
    
    result = find_extrema(0.4, data)
    
    assert result == [
        (0, ExtremumType.Max),
        (1, ExtremumType.Min),
        (2, ExtremumType.Max),
        (3, ExtremumType.Single),
    ]
    
    data.append(TimeValue(1.4, Decimal("13.37")))
    result = find_extrema(0.3, data)
    assert result == [
        (0, ExtremumType.Max),
        (1, ExtremumType.Min),
        (2, ExtremumType.Max),
        (3, ExtremumType.Single),
        (4, ExtremumType.Single),
    ]
    
    data.append(TimeValue(1.6, Decimal("13.57")))
    result = find_extrema(0.4, data)
    assert result == [
        (0, ExtremumType.Max),
        (1, ExtremumType.Min),
        (2, ExtremumType.Max),
        (3, ExtremumType.Single),
        (5, ExtremumType.Max),
    ]
