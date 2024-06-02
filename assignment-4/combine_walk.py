import pandas as pd
import xml.etree.ElementTree as et
import functools as ft
from operator import methodcaller


def get_lat_lon_and_date(file_name: str) -> pd.DataFrame:
    """
    @param file_name: the name of a xml file containing a collection of observations where each one has a
     latitude, longitude, and the date
    @return: returns the latitude, longitude, and date of each observation as different columns within a
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


def averages_in_nearest_four_seconds(df: pd.DataFrame) -> pd.DataFrame:
    """
    @param df: a DataFrame with rows containing the time, and other data
    @return: groups the rows to the nearest four seconds and takes the averages of the other data in the groups. Returns
    a DataFrame containing the averages of the grouped rows
    """
    df_cpy = df.copy()
    df_cpy["date"] = df_cpy["date"].dt.round("4s")
    return df_cpy.groupby(["date"]).mean()


def best_offset(phone_data: pd.DataFrame, accelerometer_data: pd.DataFrame, offsets):
    """
    @param phone_data: a DataFrame with rows containing the time (in terms of seconds from beginning),
     x coordinate, y coordinate, and gFx
    @param accelerometer_data: a DataFrame with rows containing the date and the acceleration in the x dimension
    @param offsets: a collection of numbers representing how much to offset the time in phone_data by
    @return: the offset corresponding with the highest cross-correlation between the gFx values in phone_data
    and the x-axis acceleration values in accelerometer_data after applying the offset to the time in phone_data
    """
    correlation_and_offset = ft.partial(correlation_value, phone_data, accelerometer_data)
    correlation_values_and_offsets = map(correlation_and_offset, offsets)
    # phone_cpy = phone_data.copy()
    # phone_cpy['timestamp'] = accelerometer_data["date"].min() + pd.to_timedelta(phone_cpy['time'] + offset, unit='sec')


def correlation_value(phone_data: pd.DataFrame, accelerometer_data: pd.DataFrame, offset):
    """
    @param phone_data: a DataFrame with rows containing the time (in terms of seconds from beginning),
     x coordinate, y coordinate, and gFx
    @param accelerometer_data: a DataFrame with rows containing the date and the acceleration in the x dimension
    @param offset: the amount of time to offset the time in phone_data by
    @return: the cross-correlation between the gFx values in phone_data and the x-axis acceleration values in
    accelerometer data
    """
    def add_offset(df: pd.DataFrame):
        cpy = df.copy()
        cpy['date'] = accelerometer_data['date'].min() + pd.to_timedelta(cpy['date'] + offset, unit='sec')
        return cpy

    join_on_date = methodcaller("merge", accelerometer_data.set_index('date'), on="date")

    def calc_cross_correlation(df):
        """
        @param df: a DataFrame with the x coordinates and the x-axis acceleration as columns
        @return: the dot product of the x coordinates and the x-axis acceleration
        """
        return None if df.empty else df['x'].dot(df['gFx'])

    phone_cpy = phone_data.copy().rename(columns={"time": "date"})

    return (phone_cpy.pipe(add_offset).
            pipe(averages_in_nearest_four_seconds).
            pipe(join_on_date).
            pipe(calc_cross_correlation))

