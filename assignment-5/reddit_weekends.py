import pandas as pd
import os
import sys
from scipy import stats
from matplotlib import pyplot as plt
from matplotlib.pyplot import hist
import numpy as np
import operator as op


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


def comments_only_in_canada_subreddit(comments: pd.DataFrame) -> pd.DataFrame:
    """
    @param comments: a DataFrame containing the date, subreddit name, and comment count in each row
    @return: a DataFrame containing only the comments made in the r/canada subreddit
    """
    return comments.query('subreddit == "canada"')


def valid_comments(comments: pd.DataFrame):
    return comments_only_in_canada_subreddit(
        comments_only_in_2012_or_2013(comments)
    )


def get_normality_value_after_applying_f(f, weekday_comments, weekend_comments):
    """
    @param f:
    @param weekday_comments:
    @param weekend_comments:
    @return:
    """
    transformed_wkday_comments = f(weekday_comments)
    transformed_wkend_comments = f(weekend_comments)
    wkday_pvalue, wkend_pvalue = (stats.normaltest(transformed_wkday_comments).pvalue,
                                  stats.normaltest(transformed_wkend_comments).pvalue)
    return wkday_pvalue, wkend_pvalue


def mean_of_comment_counts(comments: pd.DataFrame) -> pd.Series:
    """
    @param comments: a DataFrame containing the date, subreddit name, and comment count in each row
    @return: a Series containing the averages of the comments occurring in the same week and
    year
    """
    def year_and_week(date):
        """
        @param date: the date corresponding to a comment posted on a subreddit
        @return: the year and week the comment was posted
        """
        return date.isocalendar()[0:2]

    return comments.set_index('date').groupby(year_and_week).agg({"comment_count": 'mean'})['comment_count']




if not os.getenv('TESTING'):
    comment_file = sys.argv[1]
    reddit_comments = pd.read_json(comment_file, lines=True)
    filtered_comments = valid_comments(reddit_comments)
    # print(reddit_comments)
    # print(filtered_comments)
    wkday_comments, wkend_comments = separate_weekends_and_weekdays(filtered_comments)
    # pval = stats.ttest_ind(wkend_comments, wkend_comments).pvalue
    # print(pval)
    # wkday_normality, wkend_normality = stats.normaltest(wkday_comments).pvalue, stats.normaltest(wkend_comments).pvalue
    # print(wkday_normality)
    # print(wkend_normality)
    # equal_var = stats.levene(wkday_comments, wkend_comments).pvalue

