import pandas as pd
import create_plots as cp
import numpy as np
import numpy.testing as npt
import pandas.testing as pdt

expected_views = [10, 2, 1]

sample = pd.Series(data=[1, 10, 2], index=["a", "b", "c"])


def test_sort_descending():
    npt.assert_equal(cp.sort_descending(sample), expected_views)


expected_views_fst_hr = pd.Series([1], index=["Athol_Moffitt"])
expected_views_snd_hr = pd.Series([3], index=["Athol_Moffitt"])
sample_frame_1 = pd.DataFrame({"page": ["Athol_Moffitt", "b"], 'lang': ['en', 'en'], 'views': [1, 2], 'bytes': [0, 0]}).set_index("page")
sample_frame_2 = pd.DataFrame({"page": ["Athol_Moffitt"], 'lang': ['en'], 'views': [3], 'bytes': [0]}).set_index("page")


actual_views_fst_hr, actual_views_snd_hr = cp.views_of_pages_common_to_both_files(sample_frame_1, sample_frame_2)


def test_views_of_pages_common_to_both_files():
    pdt.assert_series_equal(expected_views_fst_hr, actual_views_fst_hr, check_names=False)
    pdt.assert_series_equal(expected_views_snd_hr, actual_views_snd_hr, check_names=False)
