import numpy
import pandas as pd
import pandas.testing as pdt
import numpy as np
import temperature_correlation as tc

sample_cities = pd.DataFrame({"name": ["a", "b", "c", "d"],
                              "population": [1, 2, numpy.nan, 4],
                              "area": [1, numpy.nan, 3, 10001],
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


sample_city = pd.Series({"name": "a",
                         "population": 1,
                         "area": 1,
                         "latitude": 1,
                         "longitude": 1})

sample_stations = pd.DataFrame({"observations": [1, 1, 1],
                                "avg_tmax": [1, 2, 3],
                                "station": ["a1", "a2", "a3"],
                                "latitude": [0, 0, 3],
                                "longitude": [0, 2, 4],
                                "elevation": [1, 3, 8]})

expected_station = pd.DataFrame({"observations": [1],
                                 "avg_tmax": [1],
                                 "station": ["a1"],
                                 "latitude": [0],
                                 "longitude": [0],
                                 "elevation": [1]})


sample_station = pd.Series({"observations": 1,
                            "avg_tmax": 1,
                            "station": "a1",
                            "latitude": 0,
                            "longitude": 0,
                            "elevation": 1})

def test_distance():
    assert round(tc.distance([1, 1], sample_station)) == 157249


def test_closest_station():
    pdt.assert_frame_equal(tc.closest_station(sample_city, sample_stations), expected_station)