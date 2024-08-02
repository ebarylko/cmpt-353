from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import split, explode
from os import getenv
import re
import string
from sys import argv


def extract_words_from_sentences(data: DataFrame) -> DataFrame:
    """
    @param data: a DataFrame where each row contains a sentence of text
    @return: Extracts the words in each row and returns a DataFrame
    where each row is a word previously extracted
    """
    white_space_or_punctuation = r'[%s\s]+' % (re.escape(string.punctuation),)
    all_words = data.select(explode(split(data.sentences, white_space_or_punctuation)).alias('words'))
    is_not_empty = all_words.words != ''
    return all_words.filter(is_not_empty)


if not getenv('TESTING'):
    spark = SparkSession.builder.getOrCreate()
    assert len(argv) == 3, "Intended use of wordcount.py: spark-submit wordcount.py input_directory output_directory"

    input_dir = argv[1]

    # data = spark.read.text(input_dir)
    # data2 = data.select(functions.split(data.value, white_space_or_punctuation).alias('words'))
    # data3 = data2.select(functions.explode(data2.words))
    # data3.show(100)

