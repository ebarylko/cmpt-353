import reddit_weekends as rw
import pandas as pd
import pandas.testing as pdt
import operator as op
import toolz as tz
import numpy as np
import scipy.stats as st


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


sample_comments = pd.DataFrame({"date": [pd.Timestamp(2024, 1, 9),
                                         pd.Timestamp(2011, 8, 1),
                                         pd.Timestamp(2013, 9, 4),
                                         pd.Timestamp(2012, 1, 1),
                                         pd.Timestamp(2014, 1, 1)],
                                "subreddit": [1, 2, 3, 4, 5],
                                "comment_count": [1, 9, 7, 3, 4]})

expected_comments = pd.DataFrame({'date': [pd.Timestamp(2013, 9, 4),
                                           pd.Timestamp(2012, 1, 1)],
                                  'subreddit': [3, 4],
                                  'comment_count': [7, 3]})


def test_comments_only_in_2012_or_2013():
    pdt.assert_frame_equal(rw.comments_only_in_2012_or_2013(sample_comments).reset_index(drop=True),
                           expected_comments)


sample_comments2 = pd.DataFrame({"date": [pd.Timestamp(2024, 1, 9),
                                          pd.Timestamp(2012, 1, 1),
                                          pd.Timestamp(2014, 1, 1)],
                                 "subreddit": ["canada", "Yukon", "Manitoba"],
                                 "comment_count": [1, 9, 7]})

comments_only_in_canada_subreddit = pd.DataFrame({"date": [pd.Timestamp(2024, 1, 9)],
                                                  "subreddit": ["canada"],
                                                  "comment_count": [1]})


def test_comments_only_in_candada_subreddit():
    pdt.assert_frame_equal(rw.comments_only_in_canada_subreddit(sample_comments2),
                           comments_only_in_canada_subreddit)


inc = tz.partial(op.add, 1)

sample_wkday_comments = pd.Series(range(29))
sample_wkend_comments = pd.Series(range(8, 49))


def test_select_f_which_maximizes_normality():
    assert rw.select_f_which_maximizes_normality((inc, np.exp),
                                                 sample_wkday_comments,
                                                 sample_wkend_comments) == inc