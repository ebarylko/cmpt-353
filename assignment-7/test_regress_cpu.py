import regress_cpu as rc
import pandas.testing as pdt
import pandas as pd


sample_data = pd.DataFrame({"timestamp": [pd.Timestamp(9, 9, 9, 9, 9),
                                          pd.Timestamp(8, 8, 8, 8, 8),
                                          pd.Timestamp(7, 7, 7, 7, 7)],
                            "temperature": [1, 2, 3],
                            "sys_load_1": [0, 7, 9],
                            "cpu_percent": [0.1, 0.2, 0.3],
                            "cpu_freq": [3, 4, 5],
                            "fan_rpm": [0, 0, 0]})


expected = pd.DataFrame({"timestamp": [pd.Timestamp(9, 9, 9, 9, 9),
                                       pd.Timestamp(8, 8, 8, 8, 8)],
                         "temperature": [1, 2],
                         "sys_load_1": [0, 7],
                         "cpu_percent": [0.1, 0.2],
                         "cpu_freq": [3, 4],
                         "fan_rpm": [0, 0],
                         "next_temp": [2, 3]})


def test_add_next_temperature():
    pdt.assert_frame_equal(expected,
                           rc.add_next_temperature(sample_data),
                           check_dtype=False)