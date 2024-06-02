"""Testing the functions used in combine_walk.py"""
import combine_walk as cb
import pandas as pd
import pandas.testing as pdt

sample_data = pd.DataFrame({"date": [pd.Timestamp(2018, 9, 9, 9, 9, 52),
                                     pd.Timestamp(2018, 9, 9, 9, 9, 51),
                                     pd.Timestamp(2018, 9, 9, 9, 9, 53),
                                     pd.Timestamp(2018, 9, 9, 9, 9, 48)
                                     ],
                            "Bx": [1, 2, 3, 9],
                            "By": [1, 2, 3, 7]})

expected_averages = pd.DataFrame({"date": [pd.Timestamp(2018, 9, 9, 9, 9, 52),
                                           pd.Timestamp(2018, 9, 9, 9, 9, 48)],
                                  "Bx": [2., 9.],
                                  "By": [2., 7.]}).set_index("date")


def test_averages_in_nearest_four_seconds():
    pdt.assert_frame_equal(expected_averages, cb.averages_in_nearest_four_seconds(sample_data),
                           check_like=True)
