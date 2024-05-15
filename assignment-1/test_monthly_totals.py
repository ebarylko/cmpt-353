import monthly_totals as mt
import pandas as pd
import pandas.testing as pt
import numpy as np
import toolz as tz


def test_date_to_month():
    assert mt.date_to_month(pd.Timestamp(2019, 9, 4)) == '2019-09'


sample_data = {"station": ["s1", "s2", "s1", "s2", "s1", "s2"],
               "name": ["a", "b", "a", "b", "a", "b"],
               "date": [pd.Timestamp(2019, 4, 5),
                        pd.Timestamp(2019, 4, 9),
                        pd.Timestamp(2019, 5, 5),
                        pd.Timestamp(2019, 5, 9),
                        pd.Timestamp(2019, 4, 19),
                        pd.Timestamp(2019, 5, 19)],
               "elevation": [0, 0, 0, 0, 0, 0],
               "latitude": [0, 0, 0, 0, 0, 0],
               "longitude": [0, 0, 0, 0, 0, 0],
               "precipitation": [7, 3, 1, 0, 1, 4]}

sample = pd.DataFrame(data=sample_data)


expected_observations = pd.DataFrame({"name": ["a", "b"], "2019-04": [2, 1], "2019-05": [1, 1]})


# print(tz.first(mt.pivot_months_pandas(sample)))
# b = sample.set_index('name')
t = mt.pivot_months_pandas(sample)
print(t.apply(print))
# print(t)

def test_pivot_months_pandas():
    assert pt.assert_frame_equal(tz.first(mt.pivot_months_pandas(sample)),
                                 expected_observations)
