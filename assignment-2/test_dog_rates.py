import numpy as np
import numpy.testing as npt
import re


def extract_rating(tweet):
    """
    Args:
        tweet: a string that optionally contains a rating of the form x/10, where x is a positive integer

    Returns: NAN if the tweet has no rating, otherwise returns the numerical value of the rating
    """
    def process_rating(rating):
        num, denom = map(float, rating.split('/'))
        return num / denom

    match = re.search(r'(\d+(\.\d+)?)/10', tweet)
    return np.nan if not match else process_rating(match.group())



def test_extract_rating():
    npt.assert_equal(extract_rating("@Wibblywobblykid no dollar sign just the whole number should work"), np.nan)
    npt.assert_equal(extract_rating("This is Oakley. He picked you some flowers. Hopes they’re your favorite color. "
                                    "If not, he can try again. 14/10 they… https://t.co/oTWddg2972"), 14 / 10)

