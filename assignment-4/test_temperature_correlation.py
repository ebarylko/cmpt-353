import pandas as pd
import pandas.testing as pdt
import numpy as np
import temperature_correlation as tc
import toolz as tz

sample_cities = pd.DataFrame({"name": ["a", "b", "c", "d"],
                              "population": [1, 2, np.nan, 4],
                              "area": [1, np.nan, 3, 10001],
                              "latitude": [1, 2, 3, 4],
                              "longitude": [1, 2, 3, 4]})

expected_cities = pd.DataFrame({"name": ["a"],
                                "population": [1],
                                "area": [1],
                                "latitude": [1],
                                "longitude": [1]})


def test_remove_invalid_cities():
    pdt.assert_frame_equal(tc.remove_invalid_cities(sample_cities), expected_cities,
                           check_dtype=False)


val = tz.compose(np.cos, np.radians)(1)

sample_stations = pd.DataFrame({"observations": [1, 1, 1],
                                "avg_tmax": [1, 2, 3],
                                "station": ["a1", "a2", "a3"],
                                "latitude": [0, 0, 3],
                                "longitude": [0, 2, 4],
                                "elevation": [1, 3, 8]})

expected_station = pd.Series({"observations": 1,
                              "avg_tmax": 1,
                              "station": "a1",
                              "latitude": 0,
                              "longitude": 0,
                              "elevation": 1})


sample_city = pd.Series([1, 1], index=['latitude', 'longitude'])

def test_closest_station():
    pdt.assert_series_equal(tc.closest_station(sample_stations, sample_city), expected_station,
                            check_names=False)


example_cities = pd.DataFrame({"name": ["a", "b", "c"],
                               "population": [1, 2, 3],
                               "area": [1, 4, 9],
                               "latitude": [1, 2, 3],
                               "longitude": [1, 1, 2]})

example_stations = pd.DataFrame({"observations": [1, 1],
                                 "avg_tmax": [1, 2],
                                 "station": ["a1", "a2"],
                                 "latitude": [0, 0],
                                 "longitude": [0, 2],
                                 "elevation": [1, 3]})

expected_avgs = pd.Series([1, 1, 2])


def test_avg_temperatures():
    pdt.assert_series_equal(tc.avg_temperatures(example_stations, example_cities),
                            expected_avgs,
                            check_names=False)
