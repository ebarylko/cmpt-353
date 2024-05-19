import sys
import pandas as pd
import matplotlib.pyplot as plt
import os


def sort_views_descending(file_name):
    """
    Args:
        file_name: the name of a file where each line contains the name of a
        Wikipedia page and the number of times it has been viewed in the current hour

    Returns: a collection containing the number of times each page has been
    viewed sorting decreasingly
    """
    pages_and_views = pd.read_csv(file_name, sep=' ', header=None, index_col=1,
                                  names=['lang', 'page', 'views', 'bytes'])
    return pages_and_views['views'].sort_values(ascending=False).to_numpy()


if not os.getenv("TESTING"):
    plt.figure(figsize=(10, 5))
    plt.subplot(1, 2, 1)
    first_hour_views = sort_views_descending(sys.argv[1])
    plt.plot(first_hour_views)
    plt.show()



