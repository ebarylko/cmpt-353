import calc_distance as cd
import pandas as pd

def test_get_all_lat_long_time_triplets():
    assert cd.get_all_lat_long_triplets("ex.gpx") == [{"lat": 1, "lon": 2, "time": "2022-02-14"},
                                                      {"lat": 3, "lon": 4, "time": "2020-03-14"}]

