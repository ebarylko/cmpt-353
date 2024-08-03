from pyspark.sql import SparkSession, types
from chispa.dataframe_comparer import assert_df_equality
import wordcount as wc

spark = SparkSession.builder.getOrCreate()


sample_data = spark.createDataFrame([("The quick brown fox jumped over the wall",),
                                     ("",),
                                     ("Hello there!",)],
                                    schema='sentences string')


expected_data = spark.createDataFrame([("the",),
                                       ("quick",),
                                       ("brown",),
                                       ("fox",),
                                       ("jumped",),
                                       ("over",),
                                       ("the",),
                                       ("wall",),
                                       ("hello",),
                                       ("there",)],
                                      schema='word string')


def test_extract_words_from_sentences():
    assert_df_equality(expected_data, wc.extract_words_from_sentences(sample_data),
                       ignore_nullable=True,
                       ignore_row_order=True)


data = spark.createDataFrame([("the",),
                              ("quick",),
                              ("the",),
                              ("hello",),
                              ("there",)],
                             schema='word string')

expected_words = spark.createDataFrame([("the", 2),
                                        ("hello", 1),
                                        ("quick", 1),
                                        ("there", 1)],
                                       schema='word string, count long')


def test_group_words_by_occurrence():
    assert_df_equality(expected_words,
                       wc.group_words_by_occurrence(data),
                       ignore_nullable=True)
