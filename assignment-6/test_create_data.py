import create_data as cd
import pandas.testing as pdt
import pandas as pd

sample_runtimes = [[1, 2, 3],
                   [4, 5, 6]]

sample_column_names = ["a", "b"]

expected_row_names = ["dataset-1", "dataset-2", "dataset-3"]

expected_chart = pd.DataFrame({"a": [1, 2, 3],
                               "b": [4, 5, 6]},
                              index=expected_row_names)


def test_prepare_runtime_chart():
    pdt.assert_frame_equal(cd.prepare_runtime_chart(sample_runtimes, sample_column_names),
                           expected_chart)