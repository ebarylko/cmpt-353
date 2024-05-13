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


print(get_precip_data()["date"])