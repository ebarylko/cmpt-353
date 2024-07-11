from sys import argv, version_info
from pyspark.sql import SparkSession, types, DataFrame
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


def filter_valid_observations(data: DataFrame) -> DataFrame:
    """
    @param data: a DataFrame containing weather observations where each row has a station name, a date, the type of observation,
    the value associated with the observation, the mflag, the qflag, the sflag, and the obstime
    @return: a DataFrame containing observations that were reported in Canada about the maximum temperature
    """
    return data.filter((data.observation == 'TMAX') & (data.qflag.isNull()) & (data.station.startswith('CA')))


def get_station_date_and_tmax(data: DataFrame) -> DataFrame:
    """
    @param data: a DataFrame containing weather observations taken within Canada measuring the maximum temperatures
    observed multiplied by 10
    @return: a DataFrame containing only the date, station name, and actual maximum temperature associated with
    the observation
    """
    return data.select(data['station'], data['date'], (data['value'] / 10).alias("tmax"))


if not getenv('TESTING'):
    input_directory = argv[1]
    data = spark.read.csv(input_directory, schema=observation_schema)

    # data.show()
    # new_data = data.filter((data.station.startswith('CA')) & (data.observation == 'TMAX') )
    max_temperature_observations = filter_valid_observations(data)
    max_temperature_observations.show()

    # station_date_and_temp = get_station_date_and_tmax(max_temperature_observations)

    # station_date_and_temp.show()

