import time
from typing import List

Matrix = List[List[int]]


def task_1(exp: int):
    def power_factory(base):
        return base**exp

    return power_factory


def task_2(*args, **kwags):
    for i in args:
        print(i)
    for j in kwags.values():
        print(j)


def helper(func):
    def wrapper(*args, **kwargs):
        print("Hi, friend! What's your name?")
        # execute decorated function
        func(*args, **kwargs)
        print("See you soon!")

    return wrapper


@helper
def task_3(name: str):
    print(f"Hello! My name is {name}.")


def timer(func):
    def wrapper():
        start = time.time()  # start time
        func()
        end = time.time()  # end time
        run_time = end - start  # differance
        print(f"Finished {func.__name__} in {run_time:.4f} secs")

    return wrapper


@timer
def task_4():
    return len([1 for _ in range(0, 10**8)])


def task_5(matrix: Matrix) -> Matrix:
    # row count is actual length of matrix
    row = len(matrix)
    # columns can't be different so get len of [0]
    col = len(matrix[0])
    # create our new matix with 0 values
    result = [[0] * row for _ in range(col)]
    # transpose matrix
    for i in range(col):
        for j in range(row):
            result[i][j] = matrix[j][i]

    return result


task_5([[1, 2, 3], [5, 8, 9]])


def task_6(queue: str):
    chars = []

    for char in queue:
        # store all '(' chars
        if char == "(":
            chars.append(char)
        # subtract '(' char when we got ')'
        elif char == ")":
            if not chars:
                return False
            chars = chars[:-1]
    if len(chars) == 0:
        return True

    return False
