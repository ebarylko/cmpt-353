from pyspark.sql import SparkSession, DataFrame
from first_spark import gen_table
import chispa.dataframe_comparer as cd


spark = SparkSession.builder.getOrCreate()

sample_data = spark.createDataFrame([(1, 1, 1, 1),
                                     (1, 2, 3, 2),
                                     (3, 3, 3, 3)],
                                    schema=['id', 'x', 'y', 'z'])

expected_data = spark.createDataFrame([(1, 3, 2., 2),
                                      (3, 3, 3., 1)],
                                      schema=['bin', 'sum(x)', 'avg(y)', 'count(1)'])


def test_gen_table():
    cd.assert_df_equality(gen_table(sample_data), expected_data, ignore_nullable=True)

