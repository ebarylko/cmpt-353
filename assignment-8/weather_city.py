from os import getenv
from sys import argv
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import make_pipeline

model = make_pipeline(
    MinMaxScaler(),
    RandomForestClassifier()
)


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



# if not getenv('TESTING'):
#     labelled_file_name = argv[1]
#
#     labelled_data = pd.read_csv(labelled_file_name)
#
#     months_and_weather, cities = split_data(labelled_data, ['city'])
#     print(labelled_data)
#
