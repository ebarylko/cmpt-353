from os import getenv
from sys import argv
import pandas as pd


if not getenv('TESTING'):
    labelled_file_name = argv[1]

    labelled_data = pd.read_csv(labelled_file_name)
    print(labelled_data)