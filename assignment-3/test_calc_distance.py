import calc_distance as cd
import pandas as pd
import pandas.testing as pdt

cd.get_lat_lon_and_date("ex.gpx")

expected = pd.DataFrame({"lat": [1., 3.],
                         "lon": [2., 4.],
                         "date": [pd.to_datetime("2022-02-14"),
                                  pd.to_datetime("2020-03-04")]})


def test_get_lat_lon_and_date():
    pdt.assert_frame_equal(expected, cd.get_lat_lon_and_date("ex.gpx"))

# sample = [(1, 3), (2, 4), ("2022-02-14", "2020-03-04")]

# def test_lat_lon_and_date_to_pd():
#     assert lat_lon_and_date_to_pd(expected)