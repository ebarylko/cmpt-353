import numpy as np
import numpy.testing as ts
import np_summary as nps

sample_precipitation = np.array([[1, 2, 3], [3, 4, 5]])
sample_counts = np.array([[6, 9, 2], [4, 2, 5]])
larger_sample = np.array([[1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1], [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]])


def test_row_of_city_with_lowest_precipitation():
    assert nps.row_of_city_with_lowest_precipitation(sample_precipitation) == 0


def test_average_monthly_precipitation():
    ts.assert_equal(nps.average_monthly_precipitation(sample_precipitation, sample_counts), np.array([0.4, 6 / 11, 8 / 7]))


def test_average_precipitation_in_cites():
    ts.assert_equal(nps.test_average_precipitation_in_cites(larger_sample), np.array([1, 2]))