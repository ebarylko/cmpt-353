import numpy as np
import pandas as pd
import itertools as it


def get_precip_data():
    return pd.read_csv('e1/precipitation.csv', parse_dates=[2])


def date_to_month(d):
    # You may need to modify this function, depending on your data types.
    time_info = pd.Timestamp(d)
    return '%04i-%02i' % (time_info.year, time_info.month)


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
        return info.groupby(["name", "date"], as_index=False)

    def number_of_observations(observations):
        return len(list(observations))

    def apply_f_on_precipitation_info(precipitation_info, f):
        return precipitation_info.agg({"precipitation": f}).pivot(index="name", columns="date", values="precipitation")

    cleaned_data = data.pipe(keep_date_name_and_precipitation).pipe(group_by_name_and_date)
    observations_by_month = apply_f_on_precipitation_info(cleaned_data, number_of_observations)
    precipitation_by_month = apply_f_on_precipitation_info(cleaned_data, "sum")

    return precipitation_by_month, observations_by_month


totals, counts = pivot_months_pandas(get_precip_data())
totals.to_csv('totals.csv')
counts.to_csv('counts.csv')
np.savez('monthdata.npz', totals=totals.values, counts=counts.values)

