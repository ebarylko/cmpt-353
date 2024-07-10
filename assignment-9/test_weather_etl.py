import weather_etl as wl
from pyspark.sql import SparkSession
import chispa.dataframe_comparer as cd

spark = SparkSession.builder.getOrCreate()

sample_observations = spark.createDataFrame([("US1", "20161203", "WEDS", 2, "b", "NULL", "n", "1" ),
                                             ("CA1", "20161203", "TMAX", 2, "b", "NULL", "n", "1" ),
                                             ("Cb", "20161203", "TMAX", 2, "b", "NULL", "n", "1" ),
                                             ("CA2", "20161203", "SNOW", 2, "b", "NULL", "n", "1" ),
                                             ("CA3", "20161203", "TMAX", 2, "b", "a", "n", "1")],
                                            schema=["station", "date", "observation", "value", "mflag", "qflag", "sflag", "obstime"])

expected_observations = spark.createDataFrame([("CA1", "20161203", "TMAX", 2, "b", "NULL", "n", "1" )],
                                              schema=["station", "date", "observation", "value", "mflag", "qflag", "sflag", "obstime"])


expected = wl.filter_valid_observations(sample_observations)


def test_filter_valid_observations():
    cd.assert_df_equality(expected_observations, wl.filter_valid_observations(sample_observations))



sample_observations = spark.createDataFrame([("US1", "20161203", "WEDS", 2, "b", "NULL", "n", "1" ),
                                             ("CA1", "20161203", "TMAX", 2, "b", "NULL", "n", "1" )],
                                            schema=["station", "date", "observation", "value", "mflag", "qflag", "sflag", "obstime"])
