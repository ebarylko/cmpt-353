import numpy as np
import numpy.testing as npt
import re
import pandas.testing as pdt
import pandas as pd


def extract_rating(tweet):
    """
    Args:
        tweet: a string that optionally contains a rating of the form x/10, where x is a positive integer

    Returns: NAN if the tweet has no rating, otherwise returns the numerical value of the rating
    """

    def process_rating(rating):
        num, _ = map(float, rating.split('/'))
        return num

    match = re.search(r'(\d+(\.\d+)?)/10', tweet)
    return np.nan if not match else process_rating(match.group())


def test_extract_rating():
    npt.assert_equal(extract_rating("@Wibblywobblykid no dollar sign just the whole number should work"), np.nan)
    npt.assert_equal(extract_rating("This is Oakley. He picked you some flowers. Hopes they’re your favorite color. "
                                    "If not, he can try again. 14/10 they… https://t.co/oTWddg2972"), 14)


def dates_and_ratings_of_tweets(tweets):
    """
    Args:
        tweets: a DataFrame where each row contains the date a tweet was sent, the user who made
        the tweet, and the content of the tweet

    Returns: a DataFrame where each row has a date and rating of a tweet which
    contains a rating of the form x/10, where x is in [0, 25]
    """
    def validate_ratings(rating):
        return np.nan if rating > 25 else rating

    def remove_invalid_ratings(tweet_coll):
        tweets_cpy = tweet_coll.copy()
        tweets_cpy['rating'] = tweets_cpy['rating'].map(validate_ratings)
        return tweets_cpy.dropna()[['rating', 'created_at']].set_index('rating')

    tweets_with_ratings = tweets.assign(rating=tweets['text'].apply(extract_rating))
    return remove_invalid_ratings(tweets_with_ratings)


sample_tweets = pd.DataFrame({'id': [1, 2, 3],
                              'created_at': [pd.Timestamp('2018-05-11 20:36:44'),
                                             pd.Timestamp('2018-05-12 20:36:44'),
                                             pd.Timestamp('2018-05-13 20:36:44')],
                              'text': ['Hi there', 'Perfect. 10/10', 'Lol. 19/10']})

expected_dates_and_ratings = pd.DataFrame({'rating': [10, 19],
                                           'created_at': [pd.Timestamp('2018-05-12 20:36:44'),
                                                          pd.Timestamp('2018-05-13 20:36:44')]}).set_index('rating')
print(expected_dates_and_ratings)
print(dates_and_ratings_of_tweets(sample_tweets))
def test_dates_and_ratings_of_tweets():
    pdt.assert_frame_equal(dates_and_ratings_of_tweets(sample_tweets),
                           expected_dates_and_ratings,
                           check_index_type=False)
