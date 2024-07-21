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