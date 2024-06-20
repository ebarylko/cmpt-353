import pandas as pd
from statsmodels.stats.multicomp import pairwise_tukeyhsd
from matplotlib import pyplot as plt


data = pd.read_csv('data.csv')

transformed_data = pd.melt(data)
res = pairwise_tukeyhsd(transformed_data['value'], transformed_data['variable'])
print(res)
res.plot_simultaneous()
plt.show()

"""
qs4 > merge1 > qs5 > qs1 > partition_sort

Can not rank qs2 and qs3
qs4 > qs5
qs5 > partition_sort
qs5 > qs3
qs5 > qs2
qs5 > qs1

"""