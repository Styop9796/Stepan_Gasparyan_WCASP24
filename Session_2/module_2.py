from collections import defaultdict as dd
from itertools import product
from typing import Any, Dict, List, Tuple


def task_1(data_1: Dict[str, int], data_2: Dict[str, int]):
    for item in data_2:
        if item in data_1:
            data_1[item] += data_2[item]
        else:
            data_1[item] = data_2[item]
    return data_1


def task_2():
    result = {num: num * num for num in range(1, 16)}
    return result


def task_3(data: Dict[Any, List[str]]):
    letter_groups = data.values()  # get only values
    combinations = product(*letter_groups)  # unpack list into product

    # Join each tuple of letters into a string and collect them in a list
    result = ["".join(combination) for combination in combinations]
    return result


def task_4(data: Dict[str, int]):
    # sort our dict by values
    sorted_dict = sorted(data.items(), key=lambda x: x[1], reverse=True)
    if len(data) <= 3:
        result_list = [i[0] for i in sorted_dict]  # get only key
        return result_list
    else:
        result_list = [i[0] for i in sorted_dict[:3]]  # get only key
        return result_list


def task_5(data: List[Tuple[Any, Any]]) -> Dict[str, List[int]]:
    result_dict = dd(list)
    for item in data:
        result_dict[item[0]].append(item[1])
    return result_dict


def task_6(data: List[Any]):
    return list(set(data))


def task_7(words: [List[str]]) -> str:
    # take first word as a prefix
    prefix = words[0]
    for word in words[1:]:
        # if they are not same del last char
        while word[: len(prefix)] != prefix:
            prefix = prefix[:-1]
        if not prefix:
            return ""

    return prefix


def task_8(haystack: str, needle: str) -> int:
    if not needle:
        return 0
    if not haystack:
        return -1
    no_match = False

    for i in range(len(haystack) - 1):  # get indexes
        if len(needle) == 1:
            if haystack[i] == needle:
                return i
        else:
            if haystack[i : i + len(needle)] == needle:
                return i
            else:
                no_match = True
    if no_match:
        return -1


# also we can use find method =)
def task_8_easy_way(haystack: str, needle: str) -> int:
    if not needle:
        return 0
    return haystack.find(needle)
