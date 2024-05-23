import sys
import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np


def read_pages_and_views(file_name) -> pd.DataFrame:
    return pd.read_csv(file_name, sep=' ', header=None, index_col=1,
                       names=['lang', 'page', 'views', 'bytes'])


def read_csv(file_name: str) -> pd.DataFrame:
    return pd.read_csv(file_name, sep=' ', header=None, index_col=1,
                       names=['lang', 'page', 'views', 'bytes'])


def sort_descending(s: pd.Series) -> np.array:
    """
    Args:
        s: a series to sort

    Returns: the number of times each page has been viewed in decreasing order
    """
    return s.sort_values(ascending=False).to_numpy()


def views_of_pages_common_to_both_files(df_1: pd.DataFrame, df_2: pd.DataFrame):
    """
    Args:
       df_1: a Dataframe with pages viewed
       df_2: a Dataframe with pages viewed

    Both Dataframes have the following format:
    language, name, views, transferred
    Returns: the views of the Wikipedia pages which appear in both Dataframes
    """
    pages_in_both_files = (pd.merge(df_1, df_2, on="page").
                           rename(columns={"views_x": "hour_1", "views_y": "hour_2"}))
    return pages_in_both_files["hour_1"], pages_in_both_files["hour_2"]


def plot_data_with_title_and_axes(data, title, x_axis, y_axis, fmt='-'):
    """
    Args:
        data: the data to be plotted
        title: the title of the graph
        x_axis: the label for the x-axis
        y_axis: the label for the y-axis
        fmt: a collection of options controlling the appearance of the graph

    Returns: Creates a graph using the data, title, axes labels, and formatting options passed
    """
    plt.plot(*data, fmt)
    plt.xlabel(x_axis)
    plt.ylabel(y_axis)
    plt.title(title)


def plot_popularity_against_views(pages_and_views: pd.DataFrame):
    plt.figure(figsize=(10, 5))
    plt.subplot(1, 2, 1)
    views = sort_descending(pages_and_views['views'])
    plot_data_with_title_and_axes((views,),
                                  "Page popularity vs page views",
                                  "Popularity ranking",
                                  "Number of views")


def plot_fst_hr_views_against_snd_hr_views(fst_hr_pages, snd_hr_pages):
    plt.xscale("log")
    plt.yscale("log")
    views = views_of_pages_common_to_both_files(fst_hour_pages, snd_hour_pages)
    plot_data_with_title_and_axes(views,
                                  "Comparing page visits in consecutive hours",
                                  "Views in the first hour",
                                  "Views in the second hour",
                                  "o")


if not os.getenv("TESTING"):
    fst_hour_pages = read_csv(sys.argv[1])
    plot_popularity_against_views(fst_hour_pages)

    plt.subplot(1, 2, 2)

    snd_hour_pages = read_csv(sys.argv[2])
    plot_fst_hr_views_against_snd_hr_views(fst_hour_pages, snd_hour_pages)
    plt.savefig('wikipedia.png')
