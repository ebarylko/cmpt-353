from os import getenv
from sys import argv
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import MinMaxScaler
from sklearn.pipeline import make_pipeline

model = make_pipeline(
    MinMaxScaler(),
    RandomForestClassifier()
)

if not getenv('TESTING'):
    labelled_file_name = argv[1]

    labelled_data = pd.read_csv(labelled_file_name)

    months_and_weather, cities = split_data(labelled_data, ['city'])
    print(labelled_data)