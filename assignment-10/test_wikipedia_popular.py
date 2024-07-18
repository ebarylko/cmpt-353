import wikipedia_popular as wp
from pyspark.sql import SparkSession, types
from chispa.column_comparer import assert_column_equality

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


# def test_filter