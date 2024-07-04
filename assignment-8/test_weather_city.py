import weather_city as wc
import pandas as pd
import pandas.testing as pdt

sample_data = pd.DataFrame({"city": ["A", "B"],
                            "year": [1, 2],
                            "snowfall": [4, 5]})

expected_cities = pd.DataFrame({"city": ["A", "B"]})

expected_year_and_snowfall = pd.DataFrame({"year": [1, 2],
                                           "snowfall": [4, 5]})

actual_cities, actual_year_and_snowfall = wc.split_data_on_column(sample_data, 'city')


def test_split_data():
    pdt.assert_frame_equal(expected_cities, actual_cities)
    pdt.assert_frame_equal(expected_year_and_snowfall, actual_year_and_snowfall)