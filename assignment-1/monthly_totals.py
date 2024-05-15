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


def pivot_months_loops(data):
    """
    Create monthly precipitation totals for each station in the data set.

    This does it the hard way: using Pandas as a dumb data store, and iterating in Python.
    """
    # Find all stations and months in the data set.
    stations = set()
    months = set()
    for i, r in data.iterrows():
        stations.add(r['name'])
        m = date_to_month(r['date'])
        months.add(m)

    # Aggregate into dictionaries so we can look up later.
    stations = sorted(list(stations))
    row_to_station = dict(enumerate(stations))
    station_to_row = {s: i for i, s in row_to_station.items()}

    months = sorted(list(months))
    col_to_month = dict(enumerate(months))
    month_to_col = {m: i for i, m in col_to_month.items()}

    # Create arrays for the data, and fill them.
    precip_total = np.zeros((len(row_to_station), 12), dtype=np.uint)
    obs_count = np.zeros((len(row_to_station), 12), dtype=np.uint)

    for _, row in data.iterrows():
        m = date_to_month(row['date'])
        r = station_to_row[row['name']]
        c = month_to_col[m]

        precip_total[r, c] += row['precipitation']
        obs_count[r, c] += 1

    # Build the DataFrames we needed all along (tidying up the index names while we're at it).
    totals = pd.DataFrame(
        data=precip_total,
        index=stations,
        columns=months,
    )
    totals.index.name = 'name'
    totals.columns.name = 'month'

    counts = pd.DataFrame(
        data=obs_count,
        index=stations,
        columns=months,
    )
    counts.index.name = 'name'
    counts.columns.name = 'month'

    return totals, counts
