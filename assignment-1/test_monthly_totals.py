import monthly_totals as mt
import pandas as pd
import pandas.testing as pt
import numpy as np
import toolz as tz


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
               # "date": [pd.Timestamp(year=2019, month=4, day=5),
               #          pd.Timestamp(year=2019, month=4, day=9),
               #          pd.Timestamp(year=2019, month=5, day=5),
               #          pd.Timestamp(year=2019, month=5, day=9),
               #          pd.Timestamp(year=2019, month=4, day=19),
               #          pd.Timestamp(year=2019, month=5, day=19)],
               "elevation": [0, 0, 0, 0, 0, 0],
               "latitude": [0, 0, 0, 0, 0, 0],
               "longitude": [0, 0, 0, 0, 0, 0],
               "precipitation": [7, 3, 1, 0, 1, 4]}

sample = pd.DataFrame(data=sample_data)

expected_observations = pd.DataFrame({"name": ["a", "b"], "2019-04": [2, 1], "2019-05": [1, 1]}).set_index("name")

expected_precipitation = pd.DataFrame({"name": ["a", "b"], "2019-04": [8, 3], "2019-05": [1, 4]}).set_index("name")

actual_precipitation, actual_observations = mt.pivot_months_pandas(sample)


def test_pivot_months_pandas():
    pt.assert_frame_equal(actual_observations, expected_observations, check_names=False)
    pt.assert_frame_equal(actual_precipitation, expected_precipitation, check_names=False)

