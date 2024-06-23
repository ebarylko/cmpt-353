import pandas as pd
from statsmodels.stats.multicomp import pairwise_tukeyhsd


def print_tukey_statistics(file_name):
    """
    @param file_name: the name of the file where the runtimes of the algorithms on random datasets are
    stored
    @return: prints the results of applying Tukey's test on the data, showing which algorithms ran
    faster and which ones could not be ordered
    """
    data = pd.read_csv(file_name)
    transformed_data = pd.melt(data)
    res = pairwise_tukeyhsd(transformed_data['value'], transformed_data['variable'])
    print(res)


print_tukey_statistics("data.csv")
