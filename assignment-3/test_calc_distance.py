import calc_distance as cd
import pandas as pd

cd.get_lat_lon_and_date("ex.gpx")


def test_get_lat_lon_and_date():
    assert cd.get_lat_lon_and_date("ex.gpx") == [(1, 3), (2, 4), ("2022-02-14", "2020-03-04")]

