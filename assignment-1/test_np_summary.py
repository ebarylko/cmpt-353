import numpy as np
import numpy.testing as ts
import np_summary as nps
import itertools as it

data = np.load('e1/monthdata.npz')
totals = data['totals']
counts = data['counts']
sample_precipitation = np.array([[1, 2, 3], [3, 4, 5]])


def test_row_of_city_with_lowest_precipitation():
    assert nps.row_of_city_with_lowest_precipitation(sample_precipitation) == 0
    assert nps.row_of_city_with_lowest_precipitation(totals) == 8


sample_counts = np.array([[6, 9, 2], [4, 2, 5]])


def test_average_monthly_precipitation():
    ts.assert_equal(nps.average_monthly_precipitation(sample_precipitation, sample_counts),
                    np.array([0.4, 6 / 11, 8 / 7]))
    ts.assert_allclose(nps.average_monthly_precipitation(totals, counts),
                       np.array([27.77978339, 30.42629482, 29.41007194, 17.96654275, 21.34456929, 20.69498069,
                                 24.97718631, 19.85661765, 24.06563707, 44.68441065, 34.61568627, 32.36679537]))


def test_average_precipitation_in_cites():
    ts.assert_equal(nps.average_precipitation_in_cites(sample_precipitation, sample_counts),
                    np.array([6 / 17, 12 / 11]))
    ts.assert_allclose(nps.average_precipitation_in_cites(totals, counts),
                       np.array([47.77859779, 14.33333333, 39.91232877, 41.44505495, 23.4099723,  23.68144044,
                                 17.52197802, 36.52222222,  6.15426997]))


larger_sample = np.array([list(range(12)), list(it.repeat(1, 12))])
expected_quarters = np.array([[5450, 1408, 1466, 4624],
                              [189, 1339, 3148, 527],
                              [3120, 3357, 3386, 4705],
                              [4416, 3321, 2024, 5325],
                              [2024, 1498, 1721, 3208],
                              [1786, 1809, 2557, 2397],
                              [1583, 1296, 1729, 1770],
                              [4602, 1340, 1250, 5956],
                              [338, 524, 922, 450]])


def test_precipitation_quarters_for_each_city():
    ts.assert_equal(nps.precipitation_quarters_for_each_city(larger_sample), np.array([[3, 12, 21, 30], [3, 3, 3, 3]]))
    ts.assert_allclose(nps.precipitation_quarters_for_each_city(totals), expected_quarters)
