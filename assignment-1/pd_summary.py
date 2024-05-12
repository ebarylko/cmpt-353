import pandas as pd
import numpy as np
import functools as ft
import toolz as tz
import operator as op


def thread_last(val, *forms):
    def evalform_back(val, form):
        if callable(form):
            return form(val)
        if isinstance(form, tuple):
            func, args = form[0], form[1:]
            args = args + (val,)
            return func(*args)

    return ft.reduce(evalform_back, forms, val)


totals = pd.read_csv('e1/totals.csv').set_index(keys=['name'])

set_rows = op.methodcaller("set_index", keys=['name'])

all_observations = tz.thread_first(
    'e1/counts.csv',
    pd.read_csv,
    set_rows
)


def city_with_lowest_precipitation(cities):
    """
    Args:
        cities: a series of collections where each one records the monthly precipitation for a specific city

    Returns: the city with the lowest annual precipitation

    """
    return tz.compose(op.methodcaller("idxmin"),
                      op.methodcaller("apply", np.sum, axis=1))(cities)


def average_monthly_precipitation(cities, observations):
    """
    Args:
        cities: a series of collections where each one records the monthly precipitation for a specific city
        observations: a series of collections where each one represents the monthly observations for a city

    Returns: the average precipitation in all months of the year
    """
    return (cities.pipe(op.methodcaller("apply", np.sum, axis=0))
            .pipe(np.divide, observations.apply(np.sum, axis=0)))


print(city_with_lowest_precipitation(totals))
print(average_monthly_precipitation(totals, all_observations))
