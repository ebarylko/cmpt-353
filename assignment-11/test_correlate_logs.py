from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sample_data = spark.sparkContext.parallelize([""])

def test_extract_hostname_and_bytes():

