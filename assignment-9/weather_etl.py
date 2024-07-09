from sys import argv, version_info
from pyspark.sql import SparkSession, functions, types, DataFrame
from os import getenv

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+

observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
])

if not getenv('TESTING'):
    input_directory = argv[1]
    data = spark.read.csv(input_directory, schema=observation_schema)

    data.show()
