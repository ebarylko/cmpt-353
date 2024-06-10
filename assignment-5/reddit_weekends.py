import pandas as pd
import os
import sys
from scipy import stats


def comments_only_in_2012_or_2013(comments: pd.DataFrame) -> pd.DataFrame:
    """
    @param comments: a DataFrame containing the date, subreddit name, and comment count in each row
    @return: a DataFrame containing only the comments made in 2012-2013
    """
    start = pd.Timestamp(2012, 1, 1)
    end = pd.Timestamp(2014, 1, 1)
    return comments.query('@start <= date < @end')


def separate_weekends_and_weekdays(data: pd.DataFrame):
    """
    @param data: a DataFrame containing the date, subreddit name, and comment count in each row
    @return: two Series, where the first represents the comment counts of the comments made
    on weekdays, and the second representing the comment counts of comments made on weekends
    """
    def get_weekday(date):
        return date.weekday()
    cpy = data.copy()
    is_weekday_comment = cpy['date'].apply(get_weekday) < 5
    weekday_comments = cpy[is_weekday_comment]['comment_count'].reset_index(drop=True)
    weekend_comments = cpy[~is_weekday_comment]['comment_count'].reset_index(drop=True)
    return weekday_comments, weekend_comments


if not os.getenv('TESTING'):
    comment_file = sys.argv[1]
    reddit_comments = pd.read_json(comment_file, lines=True)
    # filtered_comments = comments_only_in_2013_or_2014(reddit_comments)
    # comments_in_2015 = reddit_comments['date'] > '2015-01-01'
    # print(reddit_comments[comments_in_2015])
    # wkday_comments, wkend_comments = separate_weekends_and_weekdays(reddit_comments)
    # pval = stats.ttest_ind(wkend_comments, wkend_comments).pvalue
    # print(pval)
    # wkday_normality, wkend_normality = stats.normaltest(wkday_comments).pvalue, stats.normaltest(wkend_comments).pvalue
    # print(wkday_normality)
    # print(wkend_normality)
    # equal_var = stats.levene(wkday_comments, wkend_comments)
    # print(equal_var)

