import pandas as pd
import numpy as np
import functools as ft
import toolz as tz
import operator as op

totals = pd.read_csv('e1/totals.csv').set_index(keys=['name'])


def thread_last(val, *forms):
    def evalform_back(val, form):
        if callable(form):
            return form(val)
        if isinstance(form, tuple):
            func, args = form[0], form[1:]
            args = args + (val,)
            return func(*args)

    return ft.reduce(evalform_back, forms, val)


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
    monthly_observations = observations.apply(np.sum, axis=0)

    # print(type(monthly_observations))
    # print(observations.apply(np.sum, axis=0).index)
    # print(observations.apply(np.sum, axis=0))
    # print(cities.apply(np.sum, axis=0))
    # print(np.divide(cities.apply(np.sum, axis=0), observations.apply(np.sum, axis=0)))
    # print(np.divide(monthly_observations, pd.DataFrame(np.array([1, 2, 3]), index=["Jan", "Feb", "Mar"])))
    return tz.thread_first(
        cities.apply(np.sum, axis=0),
        (np.divide, observations.apply(np.sum, axis=0))
    )


print(city_with_lowest_precipitation(totals))
