import pd_summary as pds
import pandas as pd
import pandas.testing as pdt
import numpy as np

sample_cities = pd.DataFrame(np.array([[1, 2, 3], [3, 4, 5]]), index=["a", "b"], columns=["Jan", "Feb", "Mar"])
sample_observations = pd.DataFrame([[1, 1, 1], [2, 3, 4]], columns=["Jan", "Feb", "Mar"])


def test_city_with_lowest_precipitation():
    assert pds.city_with_lowest_precipitation(sample_cities) == "a"


expected_monthly_precipitation = pd.Series(np.array([4 / 3, 3 / 2, 8 / 5]), index=["Jan", "Feb", "Mar"])


def test_average_monthly_precipitation():
    pdt.assert_series_equal(pds.average_monthly_precipitation(sample_cities, sample_observations),
                            expected_monthly_precipitation)
