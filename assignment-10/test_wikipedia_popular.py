import wikipedia_popular as wp

sample_file1 = 'file:///Users/eitanbarylko/Workbench/cmpt-353/assignment-10/pagecounts-0/pagecounts-20160801-120000'

def test_filepath_to_date():
    assert wp.filepath_to_date(sample_file1) == '20160801-12'
