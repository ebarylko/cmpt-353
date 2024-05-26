import matplotlib.pyplot as plt
import pandas as pd
from statsmodels.nonparametric.smoothers_lowess import lowess

data = pd.read_csv('sysinfo.csv', header=0, parse_dates=[0])
kalman_data = data[['temperature', 'cpu_percent', 'sys_load_1', 'fan_rpm']]


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
    plt.legend(['Measurements', 'Lowess'])
    plt.savefig('data.png')


print_data_lowess_and_kalman(data)