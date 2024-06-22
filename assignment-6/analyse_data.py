import pandas as pd
from statsmodels.stats.multicomp import pairwise_tukeyhsd


data = pd.read_csv('data.csv')
transformed_data = pd.melt(data)
res = pairwise_tukeyhsd(transformed_data['value'], transformed_data['variable'])
print(res)
