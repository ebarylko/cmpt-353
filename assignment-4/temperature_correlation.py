import os
import sys
import pandas as pd


def remove_invalid_cities(cities: pd.DataFrame) -> pd.DataFrame:
    """
    @param cities: a DataFrame where each row contains a city's name, population, area (km^2), longitude, and
    latitude
    @return: the cities which contain a valid population and area
    """
    return cities.dropna().query("area < 10000")