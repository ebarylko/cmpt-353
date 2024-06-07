import os
import sys
import pandas as pd
import numpy as np
import math as m
import functools as ft
import matplotlib.pyplot as plt


def read_weather_station_data(file_name):
    """
    @param file_name: the name of the file containing weather data
    @return: a DataFrame containing the information present in file_name
    """
    weather_data = pd.read_json(file_name, lines=True)
    weather_data['avg_tmax'] = weather_data['avg_tmax'] / 10
    return weather_data


def read_city_data(file_name):
    """
    @param file_name: the name of the file containing information about cities
    @return: a DataFrame containing the information present in file_name
    """

    def convert_to_km_squared(df: pd.DataFrame):
        cpy = df.copy()
        cpy['area'] = cpy['area'] / 1000000
        return cpy

    def add_population_density(df: pd.DataFrame):
        cpy = df.copy()
        cpy['population_density'] = cpy['population'] / cpy['area']
        return cpy

    city_info = pd.read_csv(file_name, header=0)
    return city_info.pipe(convert_to_km_squared).pipe(add_population_density)


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
    @param snd_loc: a collection containing a latitude and longitude for a location
    @return: the distance between fst_loc and snd_loc
    """

    def diff_between(x, y):
        return (x - y) / 2
        # return m.radians(x - y) / 2

    def sin_squared(x, y):
        return m.sin(diff_between(y, x)) ** 2

    earth_radius_in_meters = 6371000
    fst_lat, fst_lon = fst_loc
    snd_lat, snd_lon = snd_loc

    # fst_lat_in_rad = m.radians(fst_lat)
    # snd_lat_in_rad = m.radians(snd_lat)
    tmp = m.sqrt(
        sin_squared(fst_lat, snd_lat) + m.cos(fst_lat) * m.cos(snd_lat) * sin_squared(fst_lon, snd_lon)
    )

    dst_from_a_to_b = 2 * earth_radius_in_meters * m.asin(tmp)
    return dst_from_a_to_b


def join_lat_and_lon(df: pd.DataFrame) -> pd.Series:
    """
    @param df: a DataFrame containing longitude and latitude as its columns with additional information
    @return: a Series containing the latitude and longitude of the DataFrame
    """
    latitude = df['latitude']
    longitude = df['longitude']
    return latitude.combine(longitude, lambda x, y: [x, y])


def closest_station(stations: pd.DataFrame, city: pd.Series) -> pd.Series:
    """
    @param city: a Series containing the name of the city, longitude, latitude, and other information
    @param stations: a DataFrame where the rows contain the name of the station, longitude, latitude, elevation, average
    temperature readings, and total number of observations
    @return: the first station closest to the city
    """

    calc_distance = ft.partial(distance, [city[0], city[1]])
    lat_and_lon = join_lat_and_lon(stations)
    row_of_closest_city = lat_and_lon.apply(calc_distance).idxmin()
    return stations.iloc[row_of_closest_city]


def avg_temperatures(stations: pd.DataFrame, cities: pd.DataFrame):
    """
    @param stations: a DataFrame where each row contains the position in longitude and
    latitude of a weather station, the average temperature readings, the total
    number of observations, and the id of the weather station
    @param cities: a DataFrame where each row contains the name of a city,
    position in latitude and longitude, and population density
    @return: the average temperature associated to the closest weather
    station for each city
    """
    calc_avg_tmp = ft.partial(closest_station, stations)
    lat_and_lon = join_lat_and_lon(cities)
    return lat_and_lon.apply(calc_avg_tmp)['avg_tmax']


def plot_population_density_against_temperature(pop_density: pd.Series, average_temperatures: pd.Series, name):
    """
    @param pop_density: a Series containing  population density values,
    each representing a different city
    @param average_temperatures: a Series containing average temperature readings,
    each for a different city
    @param name: the name of the file which will contain the graph
    @return: plots a graph comparing the population density of a city against its
    average temperature
    """
    plt.figure(figsize=(12, 4))
    plt.xlabel("Avg Max Temperature (\u00b0C)")
    plt.ylabel("Population Density (people/km\u00b2)")
    plt.title("Temperature vs Population Density")
    plt.plot(average_temperatures, pop_density, 'b.')
    plt.savefig(name)


if not os.getenv("TESTING"):
    station_data = read_weather_station_data(sys.argv[1])
    city_data = read_city_data(sys.argv[2]).pipe(remove_invalid_cities)
    valid_city_data = remove_invalid_cities(city_data)

    avgs_for_all_cities = avg_temperatures(station_data, valid_city_data)
    file_name = sys.argv[3]
    plot_population_density_against_temperature(valid_city_data['population_density'],
                                                avgs_for_all_cities,
                                                file_name)
