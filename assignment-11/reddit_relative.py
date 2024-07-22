from pyspark.sql import SparkSession, types, DataFrame, functions
from os import getenv
from sys import argv

spark = SparkSession.builder.getOrCreate()


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


def subreddits_with_positive_post_score_avg(posts: DataFrame) -> DataFrame:
    """
    @param posts: a DataFrame where each row contains information about a reddit post, including the
    score, subreddit name, author, and other information
    @return: a DataFrame where each row contains the name of a subreddit and the average score of its posts
    if the average is positive.
    """

    subreddit_avgs = posts.groupby('subreddit').agg(functions.avg('score').alias('avg_score'))
    has_positve_avg_score = subreddit_avgs.avg_score > 0

    return subreddit_avgs.filter(has_positve_avg_score)


def calc_relative_score(avgs: DataFrame, posts: DataFrame) -> DataFrame:
    """
    @param avgs: a DataFrame where each row contains the name of a subreddit and the avg score of the posts in that
    subreddit
    @param posts: a DataFrame where each row contains the information about a post, including the subreddit
    it originated from and its score
    @return: a DataFrame containing only the posts pertaining to the subreddits in avgs, where each row
    has the information about a post and its relative score
    """
    return (posts.join(avgs, on='subreddit').
            withColumn('relative_score',
                       functions.col('score') / functions.col('avg_score'))
            .drop('avg_score'))


def best_post_in_each_subreddit(posts: DataFrame) -> DataFrame:
    """
    @param posts: a DataFrame where each row contains information about a Reddit post, including the
    subreddit it originated from, the score of the post, and its relative score
    @return: a DataFrame containing the posts which have the highest relative score for their subreddit
    """
    largest_rel_scores = (posts.groupby('subreddit').
                          agg(functions.max('relative_score').
                              alias('relative_score')))
    return largest_rel_scores.join(posts, on=['subreddit', 'relative_score'])



if not getenv('TESTING'):
    posts_directory = argv[1]

    data = spark.read.json(posts_directory, schema=comments_schema)

    pos_subreddit_avgs = subreddits_with_positive_post_score_avg(data)
    pos_subreddit_avgs.show()

    # posts_with_avg_and_relative_score = calc_relative_score(pos_subreddit_avgs, data)

