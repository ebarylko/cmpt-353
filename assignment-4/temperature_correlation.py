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


def calc_distance(city: pd.Series, stations: pd.DataFrame) -> pd.Series:
    """
    @param city: a Series containing the name of the city, longitude, latitude, and other information
    @param stations: a DataFrame where the rows contain the name of the station, longitude, latitude, elevation, average
    @return: a series where each entry contains the distance from the city to a specific station
    """
    def diff_between(x, y):
        return (x - y) / 2

    def sin_squared(x, y):
        return np.sin(diff_between(y, x)) ** 2

    city_lat_in_rad, city_lon_in_rad = np.radians(city.latitude), np.radians(city.longitude)
    station_lat_in_rad, station_lon_in_rad = np.radians(stations['latitude']), np.radians(stations['longitude'])

    tmp = np.sqrt(
        sin_squared(city_lat_in_rad, station_lat_in_rad)
        + np.cos(city_lat_in_rad) * np.cos(station_lat_in_rad) * sin_squared(city_lon_in_rad, station_lon_in_rad)
    )

    earth_radius_in_meters = 6371000
    dst_from_a_to_b = 2 * earth_radius_in_meters * np.arcsin(tmp)
    return dst_from_a_to_b


def closest_station(stations: pd.DataFrame, city: pd.Series) -> pd.Series:
    """
    @param city: a Series containing the name of the city, longitude, latitude, and other information
    @param stations: a DataFrame where the rows contain the name of the station, longitude, latitude, elevation, average
    temperature readings, and total number of observations
    @return: the first station closest to the city
    """

    distance_to_each_station = calc_distance(city, stations)
    row_of_closest_city = distance_to_each_station.idxmin()
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
    return cities.apply(calc_avg_tmp, axis='columns')['avg_tmax']
    # Change this so that you do the difference of the columns in order to have a speed up


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
    # file_name = sys.argv[3]
    # plot_population_density_against_temperature(valid_city_data['population_density'],
    #                                             avgs_for_all_cities,
    #                                             file_name)
