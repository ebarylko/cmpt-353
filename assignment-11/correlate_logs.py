from pyspark.sql import SparkSession, types, DataFrame, functions
from os import getenv
from sys import argv

spark = SparkSession.builder.getOrCreate()

if not getenv('TESTING'):
    rows = spark.sparkContext.textFile("nasa-logs-1")
    print(rows.collect())
