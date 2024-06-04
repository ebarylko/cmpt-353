import numpy
import pandas as pd
import pandas.testing as pdt
import numpy as np
import temperature_correlation as tc

sample_cities = pd.DataFrame({"name": ["a", "b", "c", "d"],
                              "population": [1, 2, numpy.nan, 4],
                              "area": [1, numpy.nan, 3, 10001],
                              "latitude": [1, 2, 3, 4],
                              "longitude": [1, 2, 3, 4]})

expected_cities = pd.DataFrame({"name": ["a"],
                                "population": [1],
                                "area": [1],
                                "latitude": [1],
                                "longitude": [1]})


def test_remove_invalid_cities():
    pdt.assert_frame_equal(tc.remove_invalid_cities(sample_cities), expected_cities,
                           check_dtype=False)