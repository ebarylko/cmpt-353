import sys
import pandas as pd
import matplotlib.pyplot as plt


def get_file_name(num):
    """
    Args:
        num: either 1 or 2, representing the first or second file respectively
    Returns: the first file if num is 1, the second file otherwise
    """
    return sys.argv[num]


file_name_1 = sys.argv[1]


fst_hour_views = pd.read_csv(file_name_1, sep=' ', header=None, index_col=1,
                     names=['lang', 'page', 'views', 'bytes'])

# print(fst_hour_views)

# snd_hour_views = pd.read_csv(file_name_2, sep=' ', header=None, index_col=1,
#                              names=['lang', 'page', 'views', 'bytes'])
#

plt.figure(figsize=(10, 5))

plt.subplot(1, 2, 1)
plt.plot(fst_hour_views['views'].sort_values(ascending=False).array)
plt.show()

plt.subplot(1, 2, 2)


def sort_views_descending(views):
    """
    Args:
        views: a collection of key value pairs mapping a page to the number of
        times it has been viewed in the hour

    Returns: a collection containing the number of times each page has been
    viewed sorting decreasingly
    """
    return views.sort_values(ascending=False).array
