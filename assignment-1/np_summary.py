import numpy as np
import operator as op
import functools as ft
from itertools import chain, repeat

data = np.load('monthdata.npz')
totals = data['totals']
counts = data['counts']


def row_of_city_with_lowest_precipitation(cities):
    """
    Args:
        cities: a collection of the monthly precipitation for various cities

    Returns: the index of the row corresponding to the city with the lowest yearly precipitation
    """
    return np.argmin(list
                     (map
                      (np.sum, cities)))


def average_monthly_precipitation(precipitation, observations):
    def sum_each_month(months):
        return np.sum(months, 0)

    p_by_month = sum_each_month(precipitation)
    o_by_month = sum_each_month(observations)
    return np.divide(p_by_month, o_by_month)


def average_precipitation_in_cites(cities, observations):
    """
    Args:
        cities: a series of collections containing the monthly precipitations across a full year for multiple cities
        observations: a series of collections with the number of observations per-month for each city

    Returns: returns a collection of the average precipitations for each city

    """

    def sum_each_row(rows):
        return np.sum(rows, 1)

    return sum_each_row(cities) / sum_each_row(observations)


def precipitation_quarters_for_each_city(precipitations):
    def sum_quarters(row):
        return [sum(row[i: i + 3]) for i in range(0, 12, 3)]

    return list(map(sum_quarters, precipitations))


print("Row with lowest total precipitation:\n", row_of_city_with_lowest_precipitation(totals), sep='')
print("Average precipitation in each month:\n", average_monthly_precipitation(totals, counts), sep='')
print("Average precipitation in each city:\n", average_precipitation_in_cites(totals, counts), sep='')


def format_row(width, row):
    fmt = f"{{:>{width}}}"
    fmt_row = " ".join(repeat(fmt, 4))
    return f"[{fmt_row}]".format(*row)


quarters = precipitation_quarters_for_each_city(totals)
length_of_largest_number = len(str(max(chain.from_iterable(quarters))))
info = list(map(ft.partial(format_row, length_of_largest_number), quarters))
print("Quarterly precipitation totals:")
print("[", "\n ".join(info), "]", sep='')
