import wikipedia_popular as wp

sample_file1 = 'file:///Users/eitanbarylko/Workbench/cmpt-353/assignment-10/pagecounts-0/pagecounts-20160801-120000'
sample_file2 = 'file:///Users/eitanbarylko/Workbench/cmpt-353/assignment-10/pagecounts-1/pagecounts-20160801-210000.gz'


def test_filepath_to_date():
    assert wp.filepath_to_date(sample_file1) == '20160801-12'
    assert wp.filepath_to_date(sample_file2) == '20160801-21'
