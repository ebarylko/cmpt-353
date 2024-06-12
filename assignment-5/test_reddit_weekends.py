import reddit_weekends as rw
import pandas as pd
import pandas.testing as pdt


sample_data = pd.DataFrame({"date": [pd.Timestamp(2024, 6, 3),
                                     pd.Timestamp(2024, 6, 8),
                                     pd.Timestamp(2024, 6, 3)],
                            "subreddit": [1, 2, 3],
                            "comment_count": [1, 9, 7]})

expected_weekend_comments = pd.DataFrame({"date": [pd.Timestamp(2024, 6, 8)],
                                          "subreddit": [2],
                                          "comment_count": [9]})


expected_weekday_comments = pd.DataFrame({"date": [pd.Timestamp(2024, 6, 3),
                                                   pd.Timestamp(2024, 6, 3)],
                                          "subreddit": [1, 3],
                                          "comment_count": [1, 7]})

actual_weekday_comments, actual_weekend_comments = rw.separate_weekends_and_weekdays(sample_data)


def test_separate_weekends_and_weekdays():
    pdt.assert_frame_equal(actual_weekend_comments.reset_index(drop=True), expected_weekend_comments, check_names=False)
    pdt.assert_frame_equal(actual_weekday_comments.reset_index(drop=True), expected_weekday_comments, check_names=False)


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


# def test_comments_only_in_2012_or_2013():
#     pdt.assert_frame_equal(rw.comments_only_in_2012_or_2013(sample_comments).reset_index(drop=True),
#                            expected_comments)
#
#
# sample_comments2 = pd.DataFrame({"date": [pd.Timestamp(2024, 1, 9),
#                                           pd.Timestamp(2012, 1, 1),
#                                           pd.Timestamp(2014, 1, 1)],
#                                  "subreddit": ["canada", "Yukon", "Manitoba"],
#                                  "comment_count": [1, 9, 7]})
#
# comments_only_in_canada_subreddit = pd.DataFrame({"date": [pd.Timestamp(2024, 1, 9)],
#                                                   "subreddit": ["canada"],
#                                                   "comment_count": [1]})
#
#
# def test_comments_only_in_candada_subreddit():
#     pdt.assert_frame_equal(rw.comments_only_in_canada_subreddit(sample_comments2),
#                            comments_only_in_canada_subreddit)
#
#
# sample_comments3 = pd.DataFrame({"date": [pd.Timestamp(2019, 9, 9),
#                                           pd.Timestamp(2019, 9, 10),
#                                           pd.Timestamp(2009, 9, 9),
#                                           ],
#                                  "subreddit": ["r/a", "r/b", "r/c"],
#                                  "comment_count": [4, 2, 3]})
#
# expected_comment_means = pd.Series([3, 3])
#
#
# def test_mean_of_weekend_and_weekday_comments():
#     pdt.assert_series_equal(rw.mean_of_comment_counts(sample_comments3), expected_comment_means,
#                             check_index=False,
#                             check_dtype=False,
#                             check_names=False)
#
