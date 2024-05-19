import sys
import pandas as pd

name_1 = sys.argv[1]
# name_2 = sys.argv[2]

file_1 = pd.read_csv(name_1, sep=' ', header=None, index_col=1,
                     names=['lang', 'page', 'views', 'bytes'])

print(file_1)
