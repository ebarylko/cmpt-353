import pandas as pd
import create_plots as cp
import numpy as np
import numpy.testing as npt

sample_views = pd.Series(np.array([4, 1, 9, 12]), index=['a', 'b', 'c', 'd'])


def test_sort_pages_by_view():
    npt.assert_equal(cp.sort_views_descending(sample_views), [12, 9, 4, 1])
