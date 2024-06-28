import os

# import random
from collections import Counter
from pathlib import Path
from random import choice, seed
from typing import List, Union

import requests
from requests.exceptions import RequestException

# from gensim.utils import simple_preprocess


S5_PATH = Path(os.path.realpath(__file__)).parent

PATH_TO_NAMES = S5_PATH / "names.txt"
PATH_TO_SURNAMES = S5_PATH / "last_names.txt"
PATH_TO_OUTPUT = S5_PATH / "sorted_names_and_surnames.txt"
PATH_TO_TEXT = S5_PATH / "random_text.txt"
PATH_TO_STOP_WORDS = S5_PATH / "stop_words.txt"


def task_1():
    seed(1)
    with open(PATH_TO_NAMES, "r") as names:
        sorted_names = sorted(names)
        with open(PATH_TO_SURNAMES, "r") as surnames:
            surnames_list = list(surnames)
            with open(PATH_TO_OUTPUT, "w") as names_surnames:
                for name in sorted_names:
                    names_surnames.write(
                        f"{name.strip().lower()} {choice(surnames_list)}"
                    )
    return names_surnames


def task_2(top_k: int):
    words = []

    with open(PATH_TO_TEXT, "r") as input_words:
        with open(PATH_TO_STOP_WORDS, "r") as stop_words:
            # clean , and . and store in list
            lower_words = input_words.read().lower()
            without_chars = lower_words.replace(",", " ").replace(".", " ")
            clean_words = without_chars.split()
            # store stop words in list
            clean_stop_words = stop_words.read().split()
            # filtered list
            final_list = [
                word
                for word in clean_words
                if word not in clean_stop_words and word != " "
            ]
            words = list(Counter(final_list).items())
            # order list by second parameter of tuple)
            sorted_data = sorted(words, key=lambda x: x[1], reverse=True)
    # return only top_k
    return sorted_data[:top_k]


def task_3(url: str):
    try:
        # get request
        response = requests.get(url)
        response.raise_for_status()
        return response
    except RequestException:  # raise error
        raise RequestException


def task_4(data: List[Union[int, str, float]]):
    result = 0
    for item in data:
        # if its int or float
        if isinstance(item, int) or isinstance(item, float):
            result += item
        # if str try to convert to float
        elif isinstance(item, str):
            try:
                result += float(item)
            # if can't convert to float try int
            except Exception:
                result += int(item)
        # if also can't convert to int raise error
        else:
            raise TypeError
    return result


def task_5():
    try:
        inp = input().split()
        var1 = float(inp[0])  # convert to float
        var2 = float(inp[1])
        result = var1 / var2
        print(result)
    except ZeroDivisionError:  # if var2 is 0
        print("Can't divide by zero")

    except ValueError:  # if can't convert to float
        print("Entered value is wrong")
