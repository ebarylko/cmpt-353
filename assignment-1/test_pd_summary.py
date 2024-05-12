import pd_summary as pds
import pandas as pd
import pandas.testing as pdt
import numpy as np

sample_cities = pd.DataFrame(np.array([[1, 2, 3], [3, 4, 5]]), index=["a", "b"], columns=["Jan", "Feb", "Mar"])
sample_observations = pd.DataFrame([[1, 1, 1], [2, 3, 4]], columns=["Jan", "Feb", "Mar"])
totals = pd.read_csv('e1/totals.csv').set_index(keys=['name'])


def test_city_with_lowest_precipitation():
    assert pds.city_with_lowest_precipitation(sample_cities) == "a"
    assert pds.city_with_lowest_precipitation(totals) == "YELLOWKNIFE A"


expected_small_sample_averages = pd.Series(np.array([4 / 3, 3 / 2, 8 / 5]), index=["Jan", "Feb", "Mar"])
all_observations = pd.read_csv('e1/counts.csv').set_index(keys=["name"])
actual_monthly_averages = np.array([27.779783, 30.426295, 29.410072, 17.966543, 21.344569, 20.694981, 24.977186, 19.856618, 24.065637, 44.684411, 34.615686, 32.366795])
all_months = ["2016-01", "2016-02", "2016-03", "2016-04", "2016-05", "2016-06", "2016-07", "2016-08", "2016-09", "2016-10", "2016-11", "2016-12"]
expected_large_sample_averages = pd.Series(actual_monthly_averages, index=all_months)


def test_average_monthly_precipitation():
    pdt.assert_series_equal(pds.average_monthly_precipitation(sample_cities, sample_observations),
                            expected_small_sample_averages)
    pdt.assert_series_equal(pds.average_monthly_precipitation(totals, all_observations), expected_large_sample_averages)
