from sys import argv, version_info
from pyspark.sql import SparkSession, types, functions, DataFrame
from pyspark.sql.functions import max
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
    is_secondary_page = ~((sample_pages.page_title.startswith('Special:')) | (sample_pages.page_title == 'Main_Page'))
    is_in_english = sample_pages.language == 'en'

    return sample_pages.filter(is_in_english & is_secondary_page)


def filter_pages_with_largest_hourly_views(pages: DataFrame) -> DataFrame:
    """
    @param pages: a DataFrame containing the title of the wikipedia page, the date it was accessed, and the number
    of times it was accessed, along with other information
    @return: a DataFrame containing the most viewed wikipedia pages in every hour
    """
    max_page_views_by_hour = pages.groupby('date').agg(max('times_requested').alias('max_counts'))
    return (pages.join(max_page_views_by_hour, 'date').
            filter(pages.times_requested == max_page_views_by_hour.max_counts).
            drop('max_counts'))


def date_title_and_times_requested(pages: DataFrame) -> DataFrame:
    """
    @param pages: a DataFrame where each row contains the date a wikipedia page was accessed, the title
    of the page, the number of times the page was accessed, and other information
    @return: a DataFrame containing the date the page was accessed, the title of the page, and the number of
    times the page was accessed, sorted by the date and the title ascending
    """
    return pages.sort(['date', 'page_title']).select('date', 'page_title', 'times_requested')


if not getenv('TESTING'):
    wikipedia_pages_directory = argv[1]

    wikipedia_pages = read_wikipedia_pages(wikipedia_pages_directory)

    english_and_secondary_pages = filter_english_and_secondary_pages(wikipedia_pages)

    most_viewed_pages = filter_pages_with_largest_hourly_views(english_and_secondary_pages)

    sorted_pages = date_title_and_times_requested(most_viewed_pages)

    output_directory = argv[2]
    sorted_pages.write.csv(output_directory, sep=',', mode='overwrite')
