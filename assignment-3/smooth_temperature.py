import matplotlib.pyplot as plt
import pandas as pd
from statsmodels.nonparametric.smoothers_lowess import lowess

data = pd.read_csv('sysinfo.csv', header=0, parse_dates=[0])


plt.figure(figsize=(12, 4))
plt.xticks(rotation=25)
plt.plot(data['timestamp'], data['temperature'], 'b.', alpha=0.5)
smoothed_data = lowess(data['temperature'], data['timestamp'], frac=0.17)[:, 1]
plt.plot(data['timestamp'], smoothed_data, 'r-')
plt.show()