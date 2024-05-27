import matplotlib.pyplot as plt
import pandas as pd
from statsmodels.nonparametric.smoothers_lowess import lowess
from pykalman import KalmanFilter
import numpy as np

data = pd.read_csv('sysinfo.csv', header=0, parse_dates=[0])


def plot_kalman_filtering(df: pd.DataFrame):
    """
    @param df:  with information about the temperature, cpu usage, load on the system, the fan rotations
    per minute, and the date the observation was taken
    @return: plots the results of the Kalman filtering on df
    """
    kalman_data = df[['temperature', 'cpu_percent', 'sys_load_1', 'fan_rpm']]
    tmp_deviation = kalman_data['temperature'].std()
    cpu_deviation = kalman_data['cpu_percent'].std()
    load_deviation = kalman_data['sys_load_1'].std()
    fan_deviation = kalman_data['fan_rpm'].std()

    print(kalman_data.values)
    initial_state = kalman_data.iloc[0]
    observation_covariance = np.diag([tmp_deviation, cpu_deviation, load_deviation, fan_deviation])
    transition_covariance = np.diag([2, 0.2, 0.01, 20])
    transition = [[0.94, 0.5, 0.2, -0.001], [0.1, 0.4, 2.1, 0], [0, 0, 0.94, 0], [0, 0, 0, 1]]

    kf = KalmanFilter(
        initial_state_mean=kalman_data.values,
        transition_matrices=transition,
        transition_covariance=transition_covariance,
        observation_covariance=observation_covariance
    )
    # kf_smoothed, _ = kf.smooth(kalman_data)
    # plt.plot(df['timestamp'], kf_smoothed[:, 0], 'g-')


def print_data_lowess_and_kalman(df: pd.DataFrame):
    """
    @param df: a DataFrame with information about the temperature, cpu usage, load on the system, the fan rotations
    per minute, and the date the observation was taken
    @return: plots the temperature compared against the moment the observation was taken in three different ways:
    using the normal data, using the data after using Lowess smoothing, and using the Kalman filter
    """
    plt.figure(figsize=(12, 4))
    plt.xticks(rotation=25)
    temperatures = df['temperature']
    dates = df['timestamp']
    plt.plot(dates, temperatures, 'b.', alpha=0.5)

    smoothed_data = lowess(temperatures, dates, frac=0.17)[:, 1]
    plt.plot(dates, smoothed_data, 'r-')

    plot_kalman_filtering(df)
    plt.legend(['Measurements', 'Lowess', 'Kalman'])
    plt.savefig('data.png')


print_data_lowess_and_kalman(data)
