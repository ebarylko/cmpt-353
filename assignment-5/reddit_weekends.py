import pandas as pd
import os
import sys
from scipy import stats
import numpy as np
from matplotlib.pyplot import hist
import matplotlib.pyplot as plt


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
    weekday_comments = cpy[is_weekday_comment]
    weekend_comments = cpy[~is_weekday_comment]
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


def get_normality_and_levene_value(weekday_comments: pd.Series, weekend_comments: pd.Series):
    """
    @param weekday_comments: a Series containing the comment counts of all comments made on
    weekdays
    @param weekend_comments:a Series containing the comment counts of all comments made on
    weekends
    @return: a collection of the pvalues of the normality tests for the weekday and weekend comments and
    the pvalue of the Levene test
    """
    wkday_pvalue, wkend_pvalue = (stats.normaltest(weekday_comments).pvalue,
                                  stats.normaltest(weekend_comments).pvalue)
    levene_pvalue = stats.levene(weekday_comments, weekend_comments).pvalue
    return wkday_pvalue, wkend_pvalue, levene_pvalue


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


def ttest_pval(weekday_comments, weekend_comments):
    return stats.ttest_ind(weekday_comments, weekend_comments).pvalue

OUTPUT_TEMPLATE = (
    "Initial T-test p-value: {initial_ttest_p:.3g}\n"
    "Original data normality p-values: {initial_weekday_normality_p:.3g} {initial_weekend_normality_p:.3g}\n"
    "Original data equal-variance p-value: {initial_levene_p:.3g}\n"
    "Transformed data normality p-values: {transformed_weekday_normality_p:.3g} {transformed_weekend_normality_p:.3g}\n"
    "Transformed data equal-variance p-value: {transformed_levene_p:.3g}\n"
    "Weekly data normality p-values: {weekly_weekday_normality_p:.3g} {weekly_weekend_normality_p:.3g}\n"
    "Weekly data equal-variance p-value: {weekly_levene_p:.3g}\n"
    "Weekly T-test p-value: {weekly_ttest_p:.3g}\n"
    "Mann-Whitney U-test p-value: {utest_p:.3g}"
)


def print_out_statistics(initial_data_stats, transformed_data_stats, weekly_data_stats, mann_whitney_pvalue):
    """
    @param initial_data_stats: a collection containing the normality test pvalues for the initial weekday and
    weekend comments, and the initial Levene and ttest pvalues
    @param transformed_data_stats: a collection containing the normality test pvalues for the transformed weekday and
    weekend comments, and the Levene test pvalue on the transformed dataset
    @param weekly_data_stats: the pvalues for the normality test, Levene test, and ttest after taking the
    averages of the comment counts
    @param mann_whitney_pvalue: the pvalue obtained after applying the mann-whitney U-test
    @return: prints out the information about each statistic
    """
    init_wkday_normality, init_wkend_normality, init_levene_val, init_ttest_val = initial_data_stats
    new_wkday_normality, new_wkend_normality, new_levene_val = transformed_data_stats
    weekly_wkday_normality, weekly_wkend_normality, weekly_levene_val, weekly_ttest_val =  weekly_data_stats

    print(OUTPUT_TEMPLATE.format(
        initial_ttest_p=init_ttest_val,
        initial_weekday_normality_p=init_wkday_normality,
        initial_weekend_normality_p=init_wkend_normality,
        initial_levene_p=init_levene_val,
        transformed_weekday_normality_p=new_wkday_normality,
        transformed_weekend_normality_p=new_wkend_normality,
        transformed_levene_p=new_levene_val,
        weekly_weekday_normality_p=weekly_wkday_normality,
        weekly_weekend_normality_p=weekly_wkend_normality,
        weekly_levene_p=weekly_levene_val,
        weekly_ttest_p=weekly_ttest_val,
        utest_p=mann_whitney_pvalue,
    ))


if not os.getenv('TESTING'):
    comment_file = sys.argv[1]
    reddit_comments = pd.read_json(comment_file, lines=True)
    filtered_comments = valid_comments(reddit_comments)
    wkday_comments, wkend_comments = separate_weekends_and_weekdays(filtered_comments)

    wkday_comment_counts, wkend_comment_counts = wkday_comments['comment_count'], wkend_comments['comment_count']
    original_data_statistics = (get_normality_and_levene_value(wkday_comment_counts, wkend_comment_counts) +
                                (ttest_pval(wkday_comment_counts, wkend_comment_counts),))

    transformed_wkday_counts, transformed_wkend_counts = np.sqrt(wkday_comment_counts), np.sqrt(wkend_comment_counts)
    transformed_statistics = get_normality_and_levene_value(transformed_wkday_counts, transformed_wkend_counts )

    wkday_averages, wkend_averages = mean_of_comment_counts(wkday_comments), mean_of_comment_counts(wkend_comments)
    weekly_statistics = (get_normality_and_levene_value(wkday_averages, wkend_averages) +
                         (ttest_pval(wkday_averages, wkend_averages),))

    whitney_pvalue = stats.mannwhitneyu(wkday_comment_counts, wkend_comment_counts).pvalue

    print_out_statistics(original_data_statistics, transformed_statistics, weekly_statistics, whitney_pvalue)

    hist(transformed_wkday_counts)
    hist(transformed_wkend_counts)
    plt.show()
