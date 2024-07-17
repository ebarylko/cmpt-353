from pyspark.sql import SparkSession, types
import chispa.dataframe_comparer as cd
import reddit_averages as ra

spark = SparkSession.builder.getOrCreate()

initial_data_schema = types.StructType([
    types.StructField("subreddit", types.StringType()),
    types.StructField("score", types.LongType())
])

sample_comments = spark.createDataFrame([("A", 1),
                                         ("B", 5),
                                         ("A", 3)],
                                        schema=initial_data_schema)

end_data_schema = types.StructType([
    types.StructField("subreddit", types.StringType()),
    types.StructField("avg(score)", types.DoubleType())
])

expected_comments = spark.createDataFrame([("A", 2.),
                                           ("B", 5.)],
                                          schema=end_data_schema)


def test_avg_score_for_each_subreddit():
    cd.assert_df_equality(expected_comments, ra.avg_score_for_each_subreddit(sample_comments))
