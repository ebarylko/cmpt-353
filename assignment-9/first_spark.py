import sys
from pyspark.sql import SparkSession, functions, types, DataFrame
from os import getenv


spark = SparkSession.builder.appName('first Spark app').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+


schema = types.StructType([
    types.StructField('id', types.IntegerType()),
    types.StructField('x', types.FloatType()),
    types.StructField('y', types.FloatType()),
    types.StructField('z', types.FloatType()),
])


def gen_table(info: DataFrame) -> DataFrame:
    """
    @param info: a DataFrame containing rows which have an x value, y value, and an id
    @return: a DataFrame containing the rows grouped by the new id (id modulo 10) with the sum of
    the x values, the average of the y values, and the number of rows which fall into this group
    """
    x_y_and_new_id = info.select(info['x'], info['y'], (info['id'] % 10).alias('bin'))
    return x_y_and_new_id.groupBy('bin').agg(functions.sum('x'), functions.avg('y'), functions.count('*'))


if not getenv('TESTING'):
    in_directory = sys.argv[1]

    data = spark.read.json(in_directory, schema=schema)

    aggregated_data = gen_table(data)

    sorted_and_split_data = aggregated_data.sort(aggregated_data['bin']).coalesce(2)

    out_directory = sys.argv[2]
    sorted_and_split_data.write.csv(out_directory, compression=None, mode='overwrite')
