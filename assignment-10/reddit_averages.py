from sys import argv, version_info
from pyspark.sql import SparkSession, types, DataFrame
from os import getenv

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

comments_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('author_flair_css_class', types.StringType()),
    types.StructField('author_flair_text', types.StringType()),
    types.StructField('body', types.StringType()),
    types.StructField('controversiality', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('id', types.StringType()),
    types.StructField('link_id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('parent_id', types.StringType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('score', types.LongType()),
    types.StructField('score_hidden', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('ups', types.LongType()),
])


def avg_score_for_each_subreddit(data: DataFrame) -> DataFrame:
    """
    @param data: A DataFrame where each row contains the subreddit a post came from along with its score
    @return: A DataFrame which has the avg scores of the posts in each subreddit
    """
    return data.groupby('subreddit').avg('score')


def write_to_directory(subreddit_data: DataFrame, directory: str):
    subreddit_data.write.csv(directory, mode='overwrite')


if not getenv('TESTING'):
    assert len(argv) == 3, "Usage: spark-submit reddit_averages.py input_directory prefix_output_directories"
    reddit_comments_directory = argv[1]
    data = spark.read.json(reddit_comments_directory, schema=comments_schema)

    avg_scores = avg_score_for_each_subreddit(data)

    subreddits_sorted_by_name = avg_scores.sort('subreddit')

    subreddits_sorted_by_score = avg_scores.sort('avg(score)', ascending=False)

    prefix = argv[2]
    write_to_directory(subreddits_sorted_by_name, prefix + '-subreddit')
    write_to_directory(subreddits_sorted_by_score, prefix + '-score')
