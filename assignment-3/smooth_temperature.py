import matplotlib.pyplot as plt
import pandas as pd


data = pd.read_csv('sysinfo.csv', header=0)

print(data)
plt.figure(figsize=(12, 4))
plt.plot(data['timestamp'], data['temperature'], 'b.', alpha=0.5)
plt.show()