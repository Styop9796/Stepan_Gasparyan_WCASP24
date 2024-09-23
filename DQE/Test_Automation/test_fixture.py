import time
import pytest

@pytest.fixture(scope="module", autouse=True)
def track_suite_time():
    print("Starting suite timer...")
    start_time = time.time()  
    yield 
    end_time = time.time() 
    total_time = end_time - start_time
    print(f"\nTotal suite execution time: {total_time:.2f} seconds")


@pytest.fixture()
def track_test_time():
    start_time = time.time()  
    yield 
    end_time = time.time()  
    test_time = end_time - start_time
    print(f"Test execution time: {test_time:.2f} seconds")


def add_numbers(a, b):
    return a + b


@pytest.mark.usefixtures("track_test_time")
def test_add_two_positive_numbers():
    a, b = 3, 5
    time.sleep(2)  
    result = add_numbers(a, b)
    assert result == 8


@pytest.mark.usefixtures("track_test_time")
def test_add_two_negative_numbers():
    a, b = -3, -5
    time.sleep(3) 
    result = add_numbers(a, b)
    assert result == -8


def test_add_negative_and_positive_numbers():
    a, b = -3, 5
    time.sleep(7)  
    result = add_numbers(a, b)
    assert result == 2
