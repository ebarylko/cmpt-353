import pd_summary as pds
import pandas as pd
import numpy as np
import numpy.testing as ts

sample_cities = pd.DataFrame(np.array([[1, 2, 3], [3, 4, 5]]), index=["a", "b"])

print(sample_cities)


def test_city_with_lowest_precipitation():
    assert pds.city_with_lowest_precipitation(sample_cities) == "a"

