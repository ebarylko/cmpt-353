import monthly_totals as mt
import pandas as pd
import pandas.testing as pt
import numpy as np
import toolz as tz
import itertools as it


def test_date_to_month():
    assert mt.date_to_month(pd.Timestamp(2019, 9, 4)) == '2019-09'


sample_data = {"station": ["s1", "s2", "s1", "s2", "s1", "s2"],
               "name": ["a", "b", "a", "b", "a", "b"],
               "date": ["2019-04-05",
                        "2019-04-09",
                        "2019-05-05",
                        "2019-05-09",
                        "2019-04-19",
                        "2019-05-19"],
               "elevation": [0, 0, 0, 0, 0, 0],
               "latitude": [0, 0, 0, 0, 0, 0],
               "longitude": [0, 0, 0, 0, 0, 0],
               "precipitation": [7, 3, 1, 0, 1, 4]}

sample = pd.DataFrame(data=sample_data)

expected_observations = pd.DataFrame({"name": ["a", "b"], "2019-04": [2, 1], "2019-05": [1, 2]}).set_index("name")

expected_precipitation = pd.DataFrame({"name": ["a", "b"], "2019-04": [8, 3], "2019-05": [1, 4]}).set_index("name")

actual_precipitation, actual_observations = mt.pivot_months_pandas(sample)


def test_pivot_months_pandas():
    pt.assert_frame_equal(actual_observations, expected_observations, check_names=False)
    pt.assert_frame_equal(actual_precipitation, expected_precipitation, check_names=False)


larger_sample_data = {"station": list(it.repeat("s1", 16)),
                      "name": list(it.repeat("a", 16)),
                      "date": ["2019-04-01",
                               "2019-04-02",
                               "2019-04-03",
                               "2019-04-04",
                               "2019-04-11",
                               "2019-04-12",
                               "2019-04-05",
                               "2019-04-06",
                               "2019-04-07",
                               "2019-04-08",
                               "2019-04-19",
                               "2019-04-20",
                               "2019-04-21",
                               "2019-04-22",
                               "2019-04-23",
                               "2019-04-24"],
                      "elevation": list(it.repeat(0, 16)),
                      "latitude": list(it.repeat(0, 16)),
                      "longitude": list(it.repeat(0, 16)),
                      "precipitation": [202, 206, 44, 130, 730, 0, 0, 20,
                                        260, 54, 0, 0, 0, 20, 180, 0]}

larger_sample = pd.DataFrame(data=larger_sample_data)

expected_larger_observations = pd.DataFrame({"name": ["a"], "2019-04": [16]}).set_index("name")


def test_pivot_months_pandas_on_larger_dataset():
    pt.assert_frame_equal(tz.second(mt.pivot_months_pandas(larger_sample)), expected_larger_observations,
                          check_names=False)
