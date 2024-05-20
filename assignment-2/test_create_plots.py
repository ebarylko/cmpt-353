import pandas as pd
import create_plots as cp
import numpy as np
import numpy.testing as npt
import pandas.testing as pdt

expected_views = [10, 2, 1]


def test_sort_pages_by_view():
    npt.assert_equal(cp.sort_views_descending("sample_plots_sample_1.txt"), expected_views)


expected_pages = pd.DataFrame({"page": ["Athol_Moffitt"], "lang_x": "en", "hour_1": [1], "bytes_x": 0,
                               "lang_y": "en", "hour_2": [3], "bytes_y": 0}).set_index("page")


def test_pages_common_to_both_files():
    pdt.assert_frame_equal(cp.pages_common_to_both_files("sample_plots_sample_1.txt", "sample_plots_sample_2.txt"),
                           expected_pages)
