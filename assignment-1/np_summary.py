import numpy as np
import toolz as tz
import operator as op
import functools as ft

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


def average_precipitation_in_cites(cities, observations):
    """
    Args:
        cities: a series of collections containing the monthly precipitations across a full year for multiple cities
        observations: a series of collections with the number of observations per-month for each city

    Returns: returns a collection of the average precipitations for each city

    """
    return tz.thread_last(
        cities,
        (map, np.sum),
        (zip, np.sum(observations, 1)),
        (map, lambda pair: pair[1] / pair[0]),
        list,
    )


def precipitation_quarters_for_each_city(precipitations):
    return tz.thread_last(
        precipitations,
        (map, tz.curry(tz.partition, 3)),
        (map, lambda coll: map(
            tz.curry(ft.reduce, op.add), coll)),
        (map, list),
        list
    )


print(row_of_city_with_lowest_precipitation(totals))
print(average_monthly_precipitation(totals, counts))
print(average_precipitation_in_cites(totals, counts))
print(precipitation_quarters_for_each_city(totals))

