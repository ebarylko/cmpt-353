import color_bayes as cb
import pandas as pd
import numpy as np
import pandas.testing as pdt

sample_data = pd.DataFrame({"R": [168, 37],
                            "G": [211, 32],
                            "B": [243, 40],
                            "Label": ["blue", "black"],
                            "Confidence": ["good", "horrible"]})


# expected_colors = np.array([[168 / 255, 211 / 255, 243 / 255], [37 / 255, 32 / 255, 40 / 255]])
expected_colors = pd.DataFrame({"R": [168 / 255, 37 / 255],
                                "G": [211 / 255, 32 / 255],
                                "B": [243 / 255, 40 / 255]})

expected_labels = pd.Series(["blue", "black"])

actual_colors, actual_labels = cb.prepare_data_for_model(sample_data)


def test_prepare_data_for_model():
    pdt.assert_frame_equal(expected_colors, actual_colors)
    pdt.assert_series_equal(expected_labels, actual_labels, check_names=False)