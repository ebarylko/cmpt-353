import monthly_totals as mt
import pandas as pd


def test_date_to_month():
    assert mt.date_to_month(pd.Timestamp(2019, 9, 4)) == '2019-09'