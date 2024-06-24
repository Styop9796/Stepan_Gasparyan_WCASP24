from typing import List


def task_1(array: List[int], target: int) -> List[int]:
    result_set = set()  # set to store seen values from our list
    for number in array:
        remain = target - number  # expected value
        if remain in result_set:
            return [remain, number]  # if it's in our set then we find it
        result_set.add(number)
    return []


def task_2(number: int) -> int:
    reversed_num = 0
    negative = False
    if number < 0:  # handle negative case
        negative = True
        number = -number
    while number > 0:  #
        last_digit = number % 10  # get last digit
        reversed_num = (reversed_num * 10) + last_digit  # store in revised order
        number = number // 10  # move to left
    if negative:  # if it was negative make it negative
        reversed_num = -reversed_num
    return reversed_num


def task_3(array: List[int]) -> int:
    counter = {}  # dict to count
    for num in array:
        if num in counter:  # if already added then +1
            counter[num] += 1
            if counter[num] == 2:
                return num
        else:  # add new when appeared first time
            counter[num] = 1
    return -1


def task_4(string: str) -> int:
    roman = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
    number = 0
    for i in range(len(string) - 1):  # not including last element
        # if left element smaller then we need to subtract
        if roman[string[i]] < roman[string[(i + 1)]]:
            number -= roman[string[i]]
        else: # else add value normally
            number += roman[string[i]]
    return number + roman[string[-1]]  # adding last one


def task_5(array: List[int]) -> int:
    minimum_num = array[0]  # set first element as min
    for num in array[1:]:
        if num < minimum_num:  # if current num smaller then set min
            minimum_num = num
    return minimum_num
