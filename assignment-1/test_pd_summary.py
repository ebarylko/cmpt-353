import pd_summary as pds
import pandas as pd
import numpy as np
import numpy.testing as ts

sample_cities = pd.DataFrame(np.array([[1, 2, 3], [3, 4, 5]]), index=["a", "b"], columns=["Jan", "Feb", "Mar"])
sample_observations = np.array([[1, 1, 1], [2, 3, 4]])


def test_city_with_lowest_precipitation():
    assert pds.city_with_lowest_precipitation(sample_cities) == "a"


expected_monthly_precipitation = pd.DataFrame(np.array([4 / 3, 3 / 2, 8 / 5]), index=["Jan", "Feb", "Mar"])


def test_average_monthly_precipitation():
    ts.assert_equal(pds.average_monthly_precipitation(sample_cities, sample_observations),
                    expected_monthly_precipitation)
