import numpy as np
import pandas as pd


def get_precip_data():
    return pd.read_csv('e1/precipitation.csv', parse_dates=[2])


def date_to_month(d):
    # You may need to modify this function, depending on your data types.
    return '%04i-%02i' % (d.year, d.month)


def pivot_months_pandas(data):
    """
    Pre: takes a series of collections, where each one represents an observation
    taken at a specific city containing information about the precipitation and
     elevation on the day the observation occurred
     Post: returns two datasets, one detailing the monthly number of observations per city  and
     the other describing the monthly precipitation per city
    """
    def keep_date_name_and_precipitation(info):
        info["date"] = info["date"].map(date_to_month)
        return info.drop(columns=['elevation', 'station', 'latitude', 'longitude'])

    def group_by_name_and_date(info):
        return info.groupby(["name", "date"])

    def number_of_observations(observations):
        return len(list(observations))

    return (data.pipe(keep_date_name_and_precipitation)
            .pipe(group_by_name_and_date)
            .agg({"precipitation": number_of_observations}))
