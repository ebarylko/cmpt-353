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
    @return: a DataFrame containing the rows grouped by the new id (id modulo 10) containing the sum of
    the x values and average of the y values
    """
    return (info.select(info['x'], info['y'], (info['id'] % 10).alias('bin')).
            groupBy('bin')
            .agg(functions.sum('x'), functions.avg('y'), functions.count('*')))

def main(in_directory, out_directory):
    # Read the data from the JSON files
    xyz = spark.read.json(in_directory, schema=schema)
    xyz.show(); return

    # Create a DF with what we need: x, (soon y,) and id%10 which we'll aggregate by.
    with_bins = xyz.select(
        xyz['x'],
        xyz['y'],
        (xyz['id'] % 10).alias('bin'),
    )

    #
    # # Aggregate by the bin number.
    grouped = with_bins.groupBy(with_bins['bin'])
    groups = grouped.agg(
        functions.sum(with_bins['x']),
        functions.avg(with_bins['y']),
        functions.count('*'))

    # # We know groups has <=10 rows, so it can safely be moved into two partitions.
    groups.show()
    return
    # groups = groups.sort(groups['bin']).coalesce(2)
    # groups.write.csv(out_directory, compression=None, mode='overwrite')


if not getenv('TESTING'):
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
