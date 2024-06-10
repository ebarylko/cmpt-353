import reddit_weekends as rw
import pandas as pd
import pandas.testing as pdt

sample_data = pd.DataFrame({"date": [pd.Timestamp(2024, 6, 3),
                                     pd.Timestamp(2024, 6, 8),
                                     pd.Timestamp(2024, 6, 3)],
                            "subreddit": [1, 2, 3],
                            "comment_count": [1, 9, 7]})

expected_weekend_counts = pd.Series([9])
expected_weekday_counts = pd.Series([1, 7])

actual_weekday_counts, actual_weekend_counts = rw.separate_weekends_and_weekdays(sample_data)


def test_separate_weekends_and_weekdays():
    pdt.assert_series_equal(actual_weekend_counts, expected_weekend_counts, check_names=False)
    pdt.assert_series_equal(actual_weekday_counts, expected_weekday_counts, check_names=False)
