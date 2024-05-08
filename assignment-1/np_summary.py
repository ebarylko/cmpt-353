import numpy as np
import toolz as tz

data = np.load('e1/monthdata.npz')
totals = data['totals']
counts = data['counts']


def city_with_lowest_precipitation(cities):
    """
    Args:
        cities: a collection of the monthly precipitation for various cities

    Returns: the row corresponding to the city with the lowest yearly precipitation
    """
    return tz.thread_last(
        cities,
        (map, np.sum),
        list
    )
