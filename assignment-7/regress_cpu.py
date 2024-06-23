import pandas as pd
from sklearn.linear_model import LinearRegression
from os import getenv
from sys import argv
import numpy as np
from matplotlib.pyplot import plot, figure, savefig, close, hist
from pykalman import KalmanFilter


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


def model_and_coefficients(X_data: pd.DataFrame, y_data: pd.Series):
    """
    @param X_data: A DataFrame containing the data used to train the model
    @param y_data: A Series containing the y values associated with the data in X_data
    @return: a linear regression model trained on X_data and y_data and the coefficients
    for the values in X_data
    """
    model = LinearRegression(fit_intercept=False)
    model.fit(X_data, y_data)
    return model, model.coef_


def read_file(filename: str) -> pd.DataFrame:
    return pd.read_csv(filename, parse_dates=['timestamp'])


def output_regression(coefficients):
    regress = ' + '.join(f'{coef:.3}*{col}' for col, coef in zip(X_columns, coefficients))
    print(f'next_temp = {regress}')


def smooth_test(coef, sysinfo, outfile):
    X_valid, y_valid = sysinfo[X_columns], sysinfo[y_column]

    # feel free to tweak these if you think it helps.
    transition_stddev = 0.4
    observation_stddev = 1.1

    dims = X_valid.shape[-1]
    initial = X_valid.iloc[0]
    observation_covariance = np.diag([observation_stddev, 2, 2, 1, 10]) ** 2
    transition_covariance = np.diag([transition_stddev, 80, 100, 10, 100]) ** 2

    init_transition = np.eye(4, 5, k=1)
    transition = np.concatenate((coef, init_transition), axis=0)

    kf = KalmanFilter(
        initial_state_mean=initial,
        initial_state_covariance=observation_covariance,
        observation_covariance=observation_covariance,
        transition_covariance=transition_covariance,
        transition_matrices=transition,
    )

    kalman_smoothed, _ = kf.smooth(X_valid)

    figure(figsize=(15, 6))
    plot(sysinfo['timestamp'], sysinfo['temperature'], 'b.', alpha=0.5)
    plot(sysinfo['timestamp'], kalman_smoothed[:, 0], 'g-')
    savefig(outfile)
    close()

def plot_errors(model, X_valid, y_valid):
    residuals = y_valid - model.predict(X_valid)
    hist(residuals, bins=100)
    savefig('test_errors.png')
    close()

if not getenv('TESTING'):
    training_file = argv[1]
    validation_file = argv[2]
    training_data, validation_data = map(read_file, [training_file, validation_file])
    complete_training_data, complete_validation_data = map(add_next_temperature, [training_data, validation_data])
    X_train, y_train = complete_training_data[X_columns], complete_training_data[y_column]
    X_valid, y_valid = complete_validation_data[X_columns], complete_validation_data[y_column]

    model, coefficients = model_and_coefficients(X_train, y_train)
    output_regression(coefficients)

    smooth_test([coefficients], complete_training_data, 'train.png')
    smooth_test([coefficients], complete_validation_data, 'valid.png')
    plot_errors(model, X_valid, y_valid)


    print(f"Training score: {model.score(X_train, y_train)}\nValidation score: {model.score(X_valid, y_valid)}")
