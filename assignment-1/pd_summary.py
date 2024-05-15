import pandas as pd
import numpy as np
import functools as ft
import operator as op


totals = pd.read_csv('e1/totals.csv').set_index(keys=['name'])

all_observations = pd.read_csv('e1/counts.csv').set_index(keys=["name"])


def city_with_lowest_precipitation(cities):
    """
    Args:
        cities: a series of collections where each one records the monthly precipitation for a specific city

    Returns: the city with the lowest annual precipitation

    """
    return cities.apply(np.sum, axis=1).idxmin()


def average_monthly_precipitation(cities, observations):
    """
    Args:
        cities: a series of collections where each one records the monthly precipitation for a specific city
        observations: a series of collections where each one represents the monthly observations for a city

    Returns: the average precipitation in all months of the year
    """
    def sum_columns(columns):
        return columns.apply(np.sum, "index")

    return cities.pipe(sum_columns).pipe(np.divide, sum_columns(observations))


def average_annual_precipitations(cities, observations) -> pd.Series:
    """
    Args:
        cities: a series of collections where each one contains the monthly precipitations for a city
        observations: a series of collections where each one contains the number of monthly observations per city

    Returns: the yearly annual precipitations in all the cities
    """
    def sum_rows(rows):
        return rows.apply(np.sum, 'columns')

    return sum_rows(cities) / sum_rows(observations)


print(city_with_lowest_precipitation(totals))
print(average_monthly_precipitation(totals, all_observations))
print(average_annual_precipitations(totals, all_observations))
