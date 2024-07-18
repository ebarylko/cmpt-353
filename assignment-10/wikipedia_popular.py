from sys import argv, version_info
from pyspark.sql import SparkSession, types, functions
from os import getenv
import re

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


def filepath_to_date(filepath):
    """
    @param filepath: a path to a csv file containing pagecounts-YYYYMMDD-HHMMSS* as part of the name
    @return: the YYYYMMDD-HH portion of the filename
    """
    date_of_file = re.search(r'pagecounts-(\d{8}-\d{2})\d*', filepath)
    return date_of_file.groups()[0]



if not getenv('TESTING'):
    wikipedia_pages_directory = argv[1]

    wikipedia_pages = spark.read.csv(wikipedia_pages_directory, sep=' ', schema=wikipedia_page_schema).withColumn('filename', functions.input_file_name())
    names = wikipedia_pages.select('filename').collect()
    print(names)

