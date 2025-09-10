""" This module contains utility functions used by the other modules. """

import inspect
import pandas as pd
from datetime import date


def is_list_of_tuples(lst):
    """
    Check if a list contains only tuples.

    Parameters
    ----------
    lst : list
        The list to be checked.

    Returns
    -------
    bool
        True if all elements in the list are tuples, False otherwise.
    """
    for item in lst:
        if not isinstance(item, tuple):
            return False
    return True


def camelcase_to_snakecase(string):
    """
    Converts a camelCase string to snake_case.

    Parameters
    ----------
    string : str
        The camelcase string to be converted.

    Returns
    -------
    str
        The snakecase representation of the input string.
    """
    string = string.replace(" ", "")
    snakecase = ""
    for i, char in enumerate(string):
        if char.isupper():
            if i < len(string) - 1 and string[i + 1].islower():
                snakecase += "_"
            snakecase += char.lower()
        else:
            snakecase += char
    return snakecase.lstrip("_")


def convert_to_tuples(lst: list) -> list[tuple]:
    """
    Convert a list into a list of tuples.

    Parameters:
        lst (list): The input list.

    Returns:
        list: A list of tuples where each tuple contains two consecutive elements from
                the input list. If the input list has an odd length, the last element
                is paired with itself.

    Examples:
        >>> convert_to_tuples([1, 2, 3, 4, 5])
        [(1, 2), (3, 4), (5, 5)]

        >>> convert_to_tuples([1, 2, 3, 4, 5, 6])
        [(1, 2), (3, 4), (5, 6)]
    """
    return [
        (lst[i], lst[i + 1]) if i != len(lst) - 1 else (lst[i], lst[i])
        for i in range(0, len(lst), 2)
    ]


def get_days_by_weekday(start_date: str, end_date: str, weekday: int) -> list[date]:
    """Return a list of dates between two dates that are on a given weekday.

    Parameters
    ----------
    start_date : str
        Start date in format YYYY-MM-DD
    end_date : str
        End date in format YYYY-MM-DD
    weekday : int
        The weekday to filter on. Use the first three letters of the weekday in
        English, e.g. "mon" for Monday, "tue" for Tuesday, etc.

    Returns
    -------
    list[date]
        List of dates between start_date and end_date that are on the given weekday.
    """
    days = [
        date_.date()
        for date_ in pd.date_range(start=start_date, end=end_date, freq=f"W-{weekday}")
    ]
    return days


def all_subclasses(cls) -> list[type]:
    """Get all the subclasses of a class recursively ignore abstract classes."""

    for subclass in cls.__subclasses__():
        yield from all_subclasses(subclass)
        if not inspect.isabstract(subclass):
            yield subclass
