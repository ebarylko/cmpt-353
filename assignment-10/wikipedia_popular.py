from sys import argv, version_info
from pyspark.sql import SparkSession, types, DataFrame
from os import getenv

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+

wikipedia_page_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('page_title', types.StringType()),
    types.StructField('times_requested', types.IntegerType()),
    types.StructField('bytes', types.LongType())
])

if not getenv('TESTING'):
    wikipedia_pages_directory = argv[1]

    wikipedia_pages = spark.read.csv(wikipedia_pages_directory, sep=' ', schema=wikipedia_page_schema)
    wikipedia_pages.show()