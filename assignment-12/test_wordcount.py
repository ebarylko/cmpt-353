from pyspark.sql import SparkSession, types
from chispa.dataframe_comparer import assert_df_equality
import wordcount as wc

spark = SparkSession.builder.getOrCreate()


sample_data = spark.createDataFrame([("The quick brown fox jumped over the wall",),
                                     ("",),
                                     ("Hello there!",)],
                                    schema='sentences string')


expected_data = spark.createDataFrame([("The",),
                                       ("quick",),
                                       ("brown",),
                                       ("fox",),
                                       ("jumped",),
                                       ("over",),
                                       ("the",),
                                       ("wall",),
                                       ("Hello",),
                                       ("there",)],
                                      schema='words string')


def test_extract_words_from_sentences():
    assert_df_equality(expected_data, wc.extract_words_from_sentences(sample_data),
                       ignore_nullable=True,
                       ignore_row_order=True)