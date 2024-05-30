import xml.etree.ElementTree as et
from datetime import datetime
import pandas as pd
import itertools as it
import os
import sys


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
    return compass_readings.merge(lat_lon_date, on="date")[["lat", "lon", "date", "Bx", "By"]]



def distance(df: pd.DataFrame) -> int:
    """
    @param df: a DataFrame where each row is an observation containing the latitude, longitude, x-component,
     y-component, and date
    @return: the sum of the distance between each consecutive pair of observations
    """
    latitudes = it.pairwise(df['lat'].values)
    return latitudes

if not os.getenv("TESTING"):
    compass_readings = read_compass_readings(sys.argv[2])
    lat_and_long_readings = get_lat_lon_and_date(sys.argv[1])
    merged_readings = combine_lat_lon_and_date_and_compass_readings(lat_and_long_readings, compass_readings)
