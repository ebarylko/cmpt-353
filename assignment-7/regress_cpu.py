import pandas as pd
from sklearn.linear_model import LinearRegression
from os import getenv
from sys import argv

X_columns = ['temperature', 'cpu_percent', 'fan_rpm', 'sys_load_1', 'cpu_freq']
y_column = 'next_temp'


def add_next_temperature(data: pd.DataFrame) -> pd.DataFrame:
    """
    @param data: a DataFrame where each row contains the temperature of the cpu at a specific time along with
    the system load, fan rpm, and cpu frequency
    @return: returns the passed DataFrame with a new column added to every row, where the new column contains the
    cpu temperature of the following row
    """
    def get_following_temperatures(temperatures: pd.Series) -> pd.Series:
        """
        @param temperatures: A Series containing the cpu temperatures of all the observations
        @return: a Series containing the values in temperatures moved one row upwards
        """
        return temperatures.shift(-1)

    temps = data['temperature']
    return data.assign(next_temp=get_following_temperatures(temps)).dropna()


def model_and_coefficients(training_df: pd.DataFrame, y_data: pd.Series):
    """
    @param training_df: A DataFrame containing the data used to train the model
    @param y_data: A Series containing the y values associated with the data in training_df
    @return: a pair of items, the first being the linear regression model trained on the data in
    training_df and y_data, and the second being the coefficients for the values in training_df
    """
    mod = LinearRegression(fit_intercept=False)
    mod.fit(training_df, y_data)
    return mod, mod.coef_


def read_file(filename: str) -> pd.DataFrame:
    return pd.read_csv(filename, parse_dates=['timestamp'])


if not getenv('TESTING'):
    training_file = argv[1]
    validation_file = argv[2]

    training_data, _ = map(read_file, [training_file, validation_file])

    complete_training_data = add_next_temperature(training_data)

    X_train, y_train = complete_training_data[X_columns], complete_training_data[y_column]

    model, coefficients = model_and_coefficients(X_train, y_train)

    regress = ' + '.join(f'{coef:.3}*{col}' for col, coef in zip(X_columns, coefficients))
    print(f'next_temp = {regress}')
