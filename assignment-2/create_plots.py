import sys
import pandas as pd
import matplotlib.pyplot as plt
import os


def pages_and_views(file_name):
    return pd.read_csv(file_name, sep=' ', header=None, index_col=1,
                       names=['lang', 'page', 'views', 'bytes'])


def sort_views_descending(file_name):
    """
    Args:
        file_name: the name of a file where each line contains the name of a
        Wikipedia page and the number of times it has been viewed in the current hour

    Returns: a collection containing the number of times each page has been
    viewed sorting decreasingly
    """
    return pages_and_views(file_name)['views'].sort_values(ascending=False).to_numpy()


def pages_common_to_both_files(file_1, file_2):
    """
    Args:
        file_1: a file containing the pages viewed in the first hour
        file_2: a file containing the pages viewed in the second hour

    Returns: the pages which appear in both files
    """
    pages_in_file_1 = pages_and_views(file_1)
    pages_in_file_2 = pages_and_views(file_2)
    return pd.merge(pages_in_file_1, pages_in_file_2, on="page")


if not os.getenv("TESTING"):
    plt.figure(figsize=(10, 5))
    plt.subplot(1, 2, 1)
    first_hour_views = sort_views_descending(sys.argv[1])
    plt.xlabel("Popularity ranking")
    plt.ylabel("Number of views")
    plt.title("Comparing page popularity and page views")
    plt.plot(first_hour_views)

    plt.subplot(1, 2, 2)
    pages_in_both_hours = pages_common_to_both_files(sys.argv[1], sys.argv[2])
    plt.plot(pages_in_both_hours["views_x"], pages_in_both_hours["views_y"], 'o')
    plt.xscale("log")
    plt.yscale("log")
    plt.xlabel("Views in the first hour")
    plt.ylabel("Views in the second hour")
    plt.title("Comparing the viewings of a page in consecutive hours")
    plt.show()

