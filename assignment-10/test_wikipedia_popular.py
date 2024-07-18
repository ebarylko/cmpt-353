import wikipedia_popular as wp
from pyspark.sql import SparkSession, types
from chispa.column_comparer import assert_column_equality
import chispa.dataframe_comparer as cd

spark = SparkSession.builder.getOrCreate()

sample_file1 = 'file:///Users/eitanbarylko/Workbench/cmpt-353/assignment-10/pagecounts-0/pagecounts-20160801-120000'
sample_file2 = 'file:///Users/eitanbarylko/Workbench/cmpt-353/assignment-10/pagecounts-1/pagecounts-20160801-210000.gz'
sample_file3 = 'file:///Users/eitanbarylko/Workbench/cmpt-353/assignment-10/pagecounts-1/pagecounts-20160802-220000.gz'

filenames_and_expected_dates = [(sample_file1, '20160801-12'),
                                (sample_file2, '20160801-21'),
                                (sample_file3, '20160802-22'),]

filename_expected_dates_and_actual_dates = (spark.createDataFrame(filenames_and_expected_dates, ['filename', 'expected_date']).
                                            withColumn('actual_date', wp.filepath_to_date('filename')))


def test_filepath_to_date():
    assert_column_equality(filename_expected_dates_and_actual_dates, 'expected_date', 'actual_date')


filter_schema = observation_schema = types.StructType([types.StructField('language', types.StringType()),
                                                       types.StructField('title', types.StringType()),])


sample_pages = spark.createDataFrame([("en", "Special: 1234"),
                                      ("spa", "Second"),
                                      ("en", "a"),
                                      ("en", "Main_Page")],
                                     schema=filter_schema)

expected_pages = spark.createDataFrame([("en", "a")],
                                       schema=filter_schema)

print(wp.filter_english_and_secondary_pages(sample_pages))

def test_filter_english_and_secondary_pages():
    cd.assert_df_equality(expected_pages, wp.filter_english_and_secondary_pages(sample_pages))
