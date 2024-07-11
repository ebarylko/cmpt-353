import weather_etl as wl
from pyspark.sql import SparkSession, types
import chispa.dataframe_comparer as cd

spark = SparkSession.builder.getOrCreate()

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

sample_observations = spark.createDataFrame([("US1", "20161203", "WEDS", 2, "b", None, "n", "1" ),
                                             ("CA1", "20161203", "TMAX", 2, "b", None, "n", "1" ),
                                             ("Cb", "20161203", "TMAX", 2, "b", None, "n", "1" ),
                                             ("CA2", "20161203", "SNOW", 2, "b", None, "n", "1" ),
                                             ("CA3", "20161203", "TMAX", 2, "b", "a", "n", "1")],
                                            schema=observation_schema)

expected_observations = spark.createDataFrame([("CA1", "20161203", "TMAX", 2, "b", None, "n", "1")],
                                              schema=observation_schema)


expected = wl.filter_valid_observations(sample_observations)


def test_filter_valid_observations():
    cd.assert_df_equality(expected_observations, wl.filter_valid_observations(sample_observations))


sample_data = spark.createDataFrame([("CA2", "20161203", "TMAX", 20, "b", "NULL", "n", "1" ),
                                     ("CA1", "20161203", "TMAX", 30, "b", "NULL", "n", "1" )],
                                    schema=["station", "date", "observation", "value", "mflag", "qflag", "sflag", "obstime"])


expected_data = spark.createDataFrame([("CA2", "20161203", 2.),
                                       ("CA1", "20161203", 3.)],
                                      schema=["station", "date", "tmax"])


def test_get_station_date_and_tmax():
    cd.assert_df_equality(expected_data, wl.get_station_date_and_tmax(sample_data))