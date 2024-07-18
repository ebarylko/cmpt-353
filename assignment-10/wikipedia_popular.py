from sys import argv, version_info
from pyspark.sql import SparkSession, types, functions, DataFrame
from os import getenv
import re

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert version_info >= (3, 8) # make sure we have Python 3.8+
assert spark.version >= '3.2' # make sure we have Spark 3.2+



@functions.udf(returnType=types.StringType())
def filepath_to_date(filepath):
    """
    @param filepath: a path to a csv file containing pagecounts-YYYYMMDD-HHMMSS* as part of the name
    @return: the YYYYMMDD-HH portion of the filename
    """
    date_of_file = re.search(r'pagecounts-(\d{8}-\d{2})\d*', filepath)
    return date_of_file.groups()[0]


def read_wikipedia_pages(pages_directory: str) -> DataFrame:
    """
    @param pages_directory: the name of a directory containing files where each contains rows having the language of the
    wikipedia page accessed, the name of the page, the number of times it was accessed, and how many bytes were transmitted
    @return: a DataFrame containing the previously mentioned information along with the date the page was accessed
    """
    wikipedia_page_schema = types.StructType([
        types.StructField('language', types.StringType()),
        types.StructField('page_title', types.StringType()),
        types.StructField('times_requested', types.IntegerType()),
        types.StructField('bytes', types.LongType())
    ])

    wikipedia_page_info = (spark.read.csv(pages_directory, sep=' ', schema=wikipedia_page_schema).
                           withColumn('filepath', functions.input_file_name()))
    page_info_with_date = wikipedia_page_info.withColumn('date', filepath_to_date('filepath'))
    return page_info_with_date


def filter_english_and_secondary_pages(sample_pages: DataFrame) -> DataFrame:
    """
    @param sample_pages: a DataFrame containing the title and language of a wikipedia page, along with other information
    @return: all the pages which are in english and are not the main page nor a special page
    """
    # is_secondary_page = sample_pages.page_title.startsWith('Special:') | sample_pages.page_title == 'Main_Page'
    # is_in_english = sample_pages.page_title == 'en'

    # return sample_pages.title == "Main"
    return sample_pages.filter((sample_pages.language == 'en') &
                               (sample_pages.title.startswith('Special:')))

    # return sample_pages.filter(is_in_english & is_secondary_page)


if not getenv('TESTING'):
    wikipedia_pages_directory = argv[1]

    wikipedia_pages = read_wikipedia_pages(wikipedia_pages_directory)


    # names = wikipedia_pages.select('filename').limit(10).collect()
    # print(names)

