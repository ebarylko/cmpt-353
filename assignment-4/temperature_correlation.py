import os
import sys
import pandas as pd
import math as m


def read_weather_station_data(file_name):
    """
    @param file_name: the name of the file containing weather data
    @return: a DataFrame containing the information present in file_name
    """
    return pd.read_json(file_name, lines=True)

def read_city_data(file_name):
    """
    @param file_name: the name of the file containing information about cities
    @return: a DataFrame containing the information present in file_name
    """
    return pd.read_csv(file_name, header=0)


def remove_invalid_cities(cities: pd.DataFrame) -> pd.DataFrame:
    """
    @param cities: a DataFrame where each row contains a city's name, population, area (km^2), longitude, and
    latitude
    @return: the cities which contain a valid population and area
    """
    return cities.dropna().query("area < 10000")


def distance(fst_loc, snd_loc):
    """
    @param fst_loc: a collection containing a latitude and longitude
    @param snd_loc: a collection containing a latitude and longitude
    @return: the distance between fst_loc and snd_loc
    """
    def diff_between(x, y):
        return m.radians(x - y) / 2

    def sin_squared(x, y):
        return m.sin(diff_between(y, x)) ** 2

    earth_radius_in_meters = 6371000
    fst_lat, fst_lon = fst_loc
    snd_lat, snd_lon = snd_loc

    fst_lat_in_rad = m.radians(fst_lat)
    snd_lat_in_rad = m.radians(snd_lat)
    tmp = m.sqrt(
        sin_squared(fst_lat, snd_lat) + m.cos(fst_lat_in_rad) * m.cos(snd_lat_in_rad) * sin_squared(fst_lon, snd_lon)
    )

    dst_from_a_to_b = 2 * earth_radius_in_meters * m.asin(tmp)
    return dst_from_a_to_b


def closest_station(city: pd.Series, stations: pd.DataFrame) -> pd.Series:
    """
    @param city: a Series containing the name of the city, longitude, latitude, and other information
    @param stations: a DataFrame where the rows contain the name of the station, longitude, latitude, elevation, average
    temperature readings, and total number of observations
    @return: the first station closest to the city
    """
    # return stations.assign(distance=)