import xml.etree.ElementTree as et
from datetime import datetime
import pandas as pd
import numpy as np
from itertools import pairwise
from functools import reduce
import os
import sys
import math as m
import matplotlib.pyplot as plt
from pykalman import KalmanFilter

ns = "{http://www.topografix.com/GPX/1/0}"


def get_lat_lon_and_date(file_name: str) -> pd.DataFrame:
    """
    @param file_name: the name of a xml file containing a collection of observations where each one has a
     latitude, longitude, and the date
    @return: returns a latitude, longitude, and date of each observation as different rows within a
    DataFrame
    """

    def lat_lon_date(observation):
        """
        @param observation: an observation containing the latitude, longitude, and date of observation
        @return: a tuple containing the latitude, longitude, and date
        """
        return (float(observation.get("lat")),
                float(observation.get("lon")),
                observation.find('{http://www.topografix.com/GPX/1/0}time').text)

    observations = et.parse(file_name).getroot().iter('{http://www.topografix.com/GPX/1/0}trkpt')
    lat_lon_and_dates_triples = map(lat_lon_date, observations)
    lat, lon, dates = list(zip(*lat_lon_and_dates_triples))
    return pd.DataFrame({"lat": lat, "lon": lon, "date": pd.to_datetime(dates, utc=True)})


def read_compass_readings(file_name) -> pd.DataFrame:
    return pd.read_csv(file_name, parse_dates=['datetime']).rename(columns={"datetime": "date"})[["date", "Bx", "By"]]


def combine_lat_lon_and_date_and_compass_readings(lat_lon_date: pd.DataFrame,
                                                  compass_readings: pd.DataFrame) -> pd.DataFrame:
    """
    @param lat_lon_date: a DataFrame containing the latitude, longitude, and date of an observation
    in each row
    @param compass_readings: a DataFrame where each row is an observation containing the date, x-coordinate,
    y-coordinate, z-coordinate, acceleration in the x, y, and z dimensions
    @return: a subset of the two DataFrames joined by the date, where each row contains the latitude,
     longitude, x-coordinate, y-coordinate, and date of an observation
    """
    return compass_readings.merge(lat_lon_date, on="date")[["lat", "lon", "Bx", "By"]]


def get_distance(locations):
    def diff_between(x, y):
        return m.radians(x - y) / 2

    earth_radius_in_meters = 6378000
    fst_lat, fst_lon = locations[0]
    snd_lat, snd_lon = locations[1]
    #
    fst_lat_in_rad = m.radians(fst_lat)
    snd_lat_in_rad = m.radians(snd_lat)
    tmp = m.sqrt(m.sin(diff_between(snd_lat, fst_lat) ** 2 +
                       m.cos(fst_lat_in_rad) * m.cos(snd_lat_in_rad) * (m.sin(diff_between(snd_lon, fst_lon)) ** 2)))
    return 2 * earth_radius_in_meters * m.asin(tmp)



def add_distance(curr_distance, consec_locations):
    """
    @param curr_distance: the current distance travelled
    @param consec_locations: a collection of two locations A and B, where each location is given in terms
    of longitude and latitude
    @return: the sum of the current distance with the distance between A and B
    """

    def diff_between(x, y):
        return m.radians(x - y) / 2

    earth_radius_in_meters = 6378000
    fst_lat, fst_lon = consec_locations[0]
    snd_lat, snd_lon = consec_locations[1]

    fst_lat_in_rad = m.radians(fst_lat)
    snd_lat_in_rad = m.radians(snd_lat)
    tmp = m.sqrt(m.sin(
        diff_between(snd_lat, fst_lat) ** 2 +
                       m.cos(fst_lat_in_rad) * m.cos(snd_lat_in_rad) * (m.sin(diff_between(snd_lon, fst_lon)) ** 2)))

    dst_from_a_to_b = 2 * earth_radius_in_meters * m.asin(tmp)
    return curr_distance + dst_from_a_to_b


def distance(df: pd.DataFrame) -> int:
    """
    @param df: a DataFrame where each row is an observation containing the latitude, longitude, x-component,
     y-component, and date
    @return: the sum of the distances between each consecutive pair of observations
    """
    latitudes = df['lat'].values
    longitudes = df['lon'].values
    consec_lat_and_lon_pairs = pairwise(zip(latitudes, longitudes))
    return reduce(add_distance, consec_lat_and_lon_pairs, 0)


def print_distance(df: pd.DataFrame):
    """
    @param df: a DataFrame where each row is an observation containing the latitude, longitude, x-component,
     y-component, and date
    @return: prints the sum of the distances between each pair of consecutive points
    """
    print(f'Unfiltered distance: {distance(df):.2f}')


def print_actual_distance(df: pd.DataFrame):
    """
    @param df: a DataFrame where each row is an observation containing the latitude, longitude, x-component,
     y-component, and date
    @return: prints the sum of the distances between each pair of consecutive points after cleaning the
    data using a Kalman filter
    """

    initial_state = df.iloc[0]
    observation_covariance = np.diag([0.1, 0.1, 4, 3]) ** 2
    transition_covariance = np.diag([0.1, 0.1, 22, 20]) ** 2
    transition = [[1, 0, 5 * pow(10, -7), 34 * m.pow(10, -7)],
                  [0, 1, -49 * m.pow(10, -7), 9 * m.pow(10, -7)],
                  [0, 0, 1, 0],
                  [0, 0, 0, 1]]
    filter = KalmanFilter(initial_state_mean=initial_state,
                          transition_matrices=transition,
                          transition_covariance=transition_covariance,
                          observation_covariance=observation_covariance)
    cleaned_data, _ = filter.smooth(df)
    print(distance(pd.DataFrame(cleaned_data, columns=["lat", "lon", "Bx", "By"])))

if not os.getenv("TESTING"):
    compass_readings = read_compass_readings(sys.argv[2])
    lat_and_long_readings = get_lat_lon_and_date(sys.argv[1])
    merged_readings = combine_lat_lon_and_date_and_compass_readings(lat_and_long_readings, compass_readings)
    # print(type(merged_readings))
    # print(merged_readings)
    print_distance(merged_readings)
    print_actual_distance(merged_readings)