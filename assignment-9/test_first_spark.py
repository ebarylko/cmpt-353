from pyspark.sql import SparkSession, DataFrame
# from pyspark.testing.utils import assertDataFrameEqual
# from first_spark import gen_table

spark = SparkSession.builder.getOrCreate()

sample_data = spark.createDataFrame([
    (1, 1, 1, 1),
    (2, 2, 2, 2),
    (3, 3, 3, 3)
],
    schema=['id', 'x', 'y', 'z'])

sample_data.show()


# def test_gen_table():
#     assertDataFrameEqual()

