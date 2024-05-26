import numpy as np
import numpy.testing as npt
import re
import pandas.testing as pdt
import pandas as pd
import numbers


def tweet_to_rating(tweet):
    """
    Args:
        tweet: a string that optionally contains a rating of the form x/10, where x is a positive integer

    Returns: NAN if the tweet has no rating, otherwise returns the numerical value of the rating
    """
    match = re.search(r'(\d+(?:\.\d+)?)/10', tweet)
    value = match and float(match.groups()[0])
    if isinstance(value, numbers.Number) and value <= 25.0: return value


def test_tweet_to_rating():
    npt.assert_equal(tweet_to_rating("@Wibblywobblykid no dollar sign just the whole number should work"), None)
    npt.assert_equal(tweet_to_rating("This is Oakley. He picked you some flowers. Hopes they’re your favorite color. "
                                     "If not, he can try again. 14/10 they… https://t.co/oTWddg2972"), 14)


def dates_and_ratings_of_valid_tweets(tweets):
    """
    Args:
        tweets: a DataFrame where each row contains the date a tweet was sent, the user who made
        the tweet, and the content of the tweet

    Returns: a DataFrame where each row has a date and rating of a tweet
    corresponding to that date which contains a rating of the form x/10,
    where x is in [0, 25]
    """
    return tweets.assign(rating=tweets['text'].apply(tweet_to_rating)).dropna()


sample_tweets = pd.DataFrame({'id': [1, 2, 3],
                              'created_at': [pd.Timestamp('2018-05-11 20:36:44'),
                                             pd.Timestamp('2018-05-12 20:36:44'),
                                             pd.Timestamp('2018-05-13 20:36:44')],
                              'text': ['Hi there', 'Perfect. 10/10', 'Lol. 19/10']})

expected_dates_and_ratings = pd.DataFrame({'id': [2, 3],
                                           'created_at': [pd.Timestamp('2018-05-12 20:36:44'),
                                                          pd.Timestamp('2018-05-13 20:36:44')],
                                           'text': ['Perfect. 10/10', 'Lol. 19/10'],
                                           'rating': [10, 19],
                                           }).set_index('rating')


def test_dates_and_ratings_of_valid_tweets():
    pdt.assert_frame_equal(dates_and_ratings_of_valid_tweets(sample_tweets).set_index('rating'),
                           expected_dates_and_ratings,
                           check_index_type=False)
