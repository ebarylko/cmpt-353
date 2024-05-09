import numpy as np
import toolz as tz
import operator as op

data = np.load('e1/monthdata.npz')
totals = data['totals']
counts = data['counts']


def row_of_city_with_lowest_precipitation(cities):
    """
    Args:
        cities: a collection of the monthly precipitation for various cities

    Returns: the row corresponding to the city with the lowest yearly precipitation
    """
    return tz.thread_last(
        cities,
        (map, np.sum),
        list,
        np.argmin,
    )


def average_monthly_precipitation(precipitation, observations):
    return tz.thread_first(
        precipitation,
        (np.sum, 0),
        (np.vectorize(op.truediv),
         np.sum(observations, 0))
    )


def test_average_precipitation_in_cites(cities):
    """
    Args:
        cities: a series of collections containing the monthly precipitations across a full year for multiple cities

    Returns: returns a collection of the average precipitations for each city

    """
    return tz.thread_first(
        cities,
        (np.sum, 1),
        list,
        (np.divide, 12)
    )


print(row_of_city_with_lowest_precipitation(totals))
print(average_monthly_precipitation(totals, counts))
# print(counts)
# print(totals)

