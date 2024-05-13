import monthly_totals as mt
import pandas as pd
import pandas.testing as pt
import numpy as np
import toolz as tz


def test_date_to_month():
    assert mt.date_to_month(pd.Timestamp(2019, 9, 4)) == '2019-09'


sample_data = {"station": ["s1", "s1", "s2", "s2"],
               "name": ["a", "a", "b", "b"],
               "date": [pd.Timestamp(2019, 9, 4),
                        pd.Timestamp(2019, 9, 5),
                        pd.Timestamp(2019, 9, 4),
                        pd.Timestamp(2019, 9, 5)],
               "precipitation": [7, 1, 3, 0]}

sample = pd.DataFrame(data=sample_data).set_index("station")


expected_observations = pd.DataFrame({"name": ["a", "b"], "2019-04": [1, 1], "2019-05": [1, 0]})


def test_pivot_months_pandas():
    assert pt.assert_frame_equal(tz.first(mt.pivot_months_pandas(sample_data)),
                                 expected_observations)