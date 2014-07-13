#!/usr/bin/python -tt

""" for now a random list of util functions which dont go
    anywhere else
"""

import re

def first_usable_value(lst, default):
    """ Given a list, returns the first non-None entry.
        Useful for selecting default values from multiple locations
    """
    try:
        return lst[next(i for i, j in enumerate(lst) if j)]
    except StopIteration:
        return default

