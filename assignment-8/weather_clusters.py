from sklearn.decomposition import PCA
from sys import argv
from os import getenv
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import make_pipeline
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt


def split_data(data: pd.DataFrame, columns_to_split_on):
    """
    @param data: a DataFrame containing data about the weather in multiple cities over a period of 55 years
    @param columns_to_split_on: a subset of the columns in data
    @return: two dataframes, the first containing the data associated with the columns in columns_to_split_on, and
    the second containing the data pertaining to all the columns not in columns_to_split_on
    """
    cpy = data.copy()
    is_part_of_columns_to_split_on = cpy.columns.isin(columns_to_split_on)
    data_associated_with_columns = cpy.loc[:, is_part_of_columns_to_split_on]
    data_not_associated_with_columns = cpy.loc[:, ~is_part_of_columns_to_split_on]
    return data_associated_with_columns, data_not_associated_with_columns


def reduce_features_down_to_two(weather_data: pd.DataFrame) -> pd.DataFrame:
    """
    @param weather_data: a DataFrame containing the weather across multiple cities over a
    period of 55 years
    @return: a DataFrame containing the two most important features in weather_data
    """
    model = make_pipeline(
        PCA(n_components=2),
        MinMaxScaler()
    )
    return model.fit_transform(weather_data)


def get_clusters(weather_data: pd.DataFrame):
    """
    @param weather_data: a DataFrame containing the weather across multiple cities over a
    period of 55 years
    @return: a collection where each entry is a prediction for the cluster the corresponding entry
    in weather_data belongs to
    """
    model = KMeans(n_clusters=9)
    return model.fit_predict(weather_data)


def plot_data_in_clusters(data: pd.DataFrame, data_in_clusters):
    """
    @param data: the information to plot
    @param data_in_clusters: the clusters to assign the points in data to
    @return: plots the datapoints in data and colors them by the cluster categories in data_in_clusters
    """
    plt.figure(figsize=(10, 6))
    plt.scatter(data[:, 0], data[:, 1], c=data_in_clusters, cmap='Set1', edgecolor='k', s=30)
    plt.savefig('clusters.png')


if not getenv('TESTING'):
    labelled_file_name = argv[1]

    labelled_data = pd.read_csv(labelled_file_name)

    cities, weather = split_data(labelled_data, ['city'])

    data_with_only_two_features = reduce_features_down_to_two(weather)

    clustered_data = get_clusters(weather)

    plot_data_in_clusters(data_with_only_two_features, clustered_data)

    counts = pd.crosstab(cities.to_numpy().ravel(), clustered_data)
    print(counts)


