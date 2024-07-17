from pyspark.sql import SparkSession, functions, types
import chispa.dataframe_comparer as cd
import reddit_averages as ra

spark = SparkSession.builder.getOrCreate()

sample_comments = spark.createDataFrame()


