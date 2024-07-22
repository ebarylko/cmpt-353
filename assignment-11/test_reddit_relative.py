from pyspark.sql import SparkSession
import reddit_relative as rr
from chispa.dataframe_comparer import assert_df_equality

spark = SparkSession.builder.getOrCreate()

sample_posts = spark.createDataFrame([("a", 1),
                                      ("b", 0),
                                      ("c", -1),
                                      ("a", 3)],
                                     ["subreddit", "score"])

expected_averages = spark.createDataFrame([("a", 2.)],
                                          ["subreddit", "avg_score"])


def test_subreddits_with_positive_post_score_avg():
    assert_df_equality(expected_averages, rr.subreddits_with_positive_post_score_avg(sample_posts))


unfiltered_posts = spark.createDataFrame([("a", 1),
                                          ("a", 3),
                                          ("b", 0),
                                          ("c", -1)],
                                         ["subreddit", "score"])

avg_scores = spark.createDataFrame([("a", 2.)],
                                   ["subreddit", "avg_score"])

expected_posts = spark.createDataFrame([("a", 1, 0.5),
                                        ("a", 3, 1.5)],
                                       ["subreddit", "score", "relative_score"])


def test_calc_relative_score():
    assert_df_equality(expected_posts, rr.calc_relative_score(avg_scores, unfiltered_posts))


sample_data = spark.createDataFrame([("a", 2, 3),
                                     ("a", 1, 2),
                                     ("b", 1, 2)],
                                    ["subreddit", "score", "relative_score"])

expected_data = spark.createDataFrame([("a", 2, 3),
                                       ("b", 1, 2)],
                                      ["subreddit", "score", "relative_score"])


def test_best_post_in_each_subreddit():
    assert_df_equality(expected_data,
                       rr.best_post_in_each_subreddit(sample_data),
                       ignore_column_order=True)
