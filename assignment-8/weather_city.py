from os import getenv
from sys import argv
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import make_pipeline


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


model = make_pipeline(
    MinMaxScaler(),
    RandomForestClassifier(n_estimators=290, min_samples_leaf=9)
)


def read_unlabelled_data(filename: str) -> pd.DataFrame:
    """
    @param filename: the name of the file containing the unlabelled data
    @return: the data within the file as a DataFrame, excluding the cities column
    """
    data = pd.read_csv(filename)
    return data.drop('city', axis='columns')


if not getenv('TESTING'):
    labelled_file_name = argv[1]

    labelled_data = pd.read_csv(labelled_file_name)

    cities, weather_data = split_data(labelled_data, ['city'])

    (training_cities, validation_cities,
     training_weather_data, validation_weather_data) = train_test_split(cities.to_numpy().ravel(), weather_data)

    model.fit(training_weather_data, training_cities)

    print("The accuracy of the model on the validation data is",
          model.score(validation_weather_data, validation_cities))

    unlabelled_file_name = argv[2]

    unlabelled_data = read_unlabelled_data(unlabelled_file_name)

    predictions = model.predict(unlabelled_data)

    file_to_save_to = argv[3]

    pd.Series(predictions).to_csv(file_to_save_to, index=False, header=False)
