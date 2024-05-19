import pandas as pd
import create_plots as cp
import numpy as np
import numpy.testing as npt
# import matplotlib as mb
# import matplotlib.pyplot as plt
import os

expected_views = [10, 2, 1]


def test_sort_pages_by_view():
    npt.assert_equal(cp.sort_views_descending("sample_plots_sample.txt"), expected_views)
