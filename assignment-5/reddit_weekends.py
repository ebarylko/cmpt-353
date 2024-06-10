import pandas as pd


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
