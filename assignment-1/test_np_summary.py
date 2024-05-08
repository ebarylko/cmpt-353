import numpy as np
import np_summary as nps

sample = np.array([[1, 2, 3], [3, 4, 5]])


def test_city_with_lowest_precipitation():
    assert nps.city_with_lowest_precipitation(sample) == 0
