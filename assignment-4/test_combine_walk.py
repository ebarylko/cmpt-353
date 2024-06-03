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


sample_phone_data = pd.DataFrame({"date": [0.3, 0.6],
                                  "gFx": [9, 3],
                                  "Bx": [1, 9],
                                  "By": [1, 6]})

sample_accelerometer_data = pd.DataFrame({"date": [pd.Timestamp(2018, 9, 9, 9, 52, 28)],
                                          "x": [3]})


def test_correlation_value():
    assert cb.correlation_value(sample_phone_data, sample_accelerometer_data, 0) == 18
    assert not cb.correlation_value(sample_phone_data, sample_accelerometer_data, 5)


sample_offsets = [-5, 0, 5]


def test_best_offset():
    assert cb.best_offset(sample_phone_data, sample_accelerometer_data, sample_offsets) == 0
    assert not cb.best_offset(sample_phone_data, sample_accelerometer_data, [5, -5])
