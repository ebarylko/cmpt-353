from implementations import all_implementations
from time import time
from numpy.random import default_rng
import pandas as pd
import functools as ft
import os


def generate_datasets(num):
    """
    @param num: the number of datasets wanted
    @return: {num} number of datasets filled with random integers from -10,000 to 10,000
    """
    seed = default_rng()
    return [seed.integers(-70000, high=70000, size=23000) for i in range(num)]


def get_runtime(f, dataset):
    """
    @param dataset: a collection of integers
    @param f: the sorting function used to order the dataset
    @return: the time it took to sort the dataset using f
    """
    start = time()
    f(dataset)
    end = time()
    return end - start


def get_runtimes(datasets, f):
    """
    @param datasets: a collection of datasets where each one contains a large number of random integers
    @param f: the sorting function to use on each dataset
    @return: a collection of times, where each one represents the time it took to apply f on
    a specific dataset
    """
    runtimes = ft.partial(get_runtime, f)
    return list(map(runtimes, datasets))


def all_runtimes(datasets, sorting_funcs):
    """
    @param datasets: a collection of datasets where each one contains a large number of random integers
    @param sorting_funcs: the sorting functions to benchmark
    @return: a collection of collections, where each subcollection contains the runtimes of a specific function
    on each of the datasets
    """
    runtimes = ft.partial(get_runtimes, datasets)
    return list(map(runtimes, sorting_funcs))


def prepare_runtime_chart(runtimes, column_names) -> pd.DataFrame:
    """
    @param runtimes: a collection of collections, where each one contains the runtimes of a specific sorting
    algorithm over the provided datasets
    @param column_names: the name of the sorting functions used on the algorithms
    @return: a DataFrame where each row has the name of one of the provided datasets and each column
    has the runtimes of a single sorting algorithm on the provided datasets
    """
    function_names_and_runtimes = dict(zip(column_names, runtimes))
    number_of_datasets = len(runtimes[0])
    dataset_names = ["dataset-{}".format(i + 1) for i in range(0, number_of_datasets)]
    return pd.DataFrame(function_names_and_runtimes, index=dataset_names)


def get_func_name(f):
    return f.__name__


if not os.getenv("TESTING"):
    all_datasets = generate_datasets(50)
    function_runtimes = all_runtimes(all_datasets, all_implementations)
    columns = map(get_func_name, all_implementations)
    runtime_chart = prepare_runtime_chart(function_runtimes, columns)
    runtime_chart.to_csv("data.csv", index=False)
