from implementations import all_implementations
from time import time
from numpy.random import randint
import pandas as pd


def get_runtime(dataset, f):
    """
    @param dataset: a collection of integers
    @param f: the sorting function used to order the dataset
    @return: the time it took to sort the dataset using f
    """
    start = time()
    f(dataset)
    end = time()
    return end - start


def prepare_runtime_chart(runtimes, column_names) -> pd.DataFrame:
    """
    @param runtimes: a collection of collections, where each one contains the runtimes of the algorithms
    over the provided datasets
    @param column_names: the name of the functions used on the algorithms
    @return: a DataFrame where each column has the runtimes of a single algorithm on the provided datasets,
    and each column has the name of one of the provided datasets
    """
    function_names_and_runtimes = dict(zip(column_names, runtimes))
    number_of_datasets = len(runtimes[0])
    dataset_names = ["dataset-{}".format(i + 1) for i in range(0, number_of_datasets)]
    return pd.DataFrame(function_names_and_runtimes, index=dataset_names)
