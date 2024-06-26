import calc_distance as cd
import pandas as pd
import pandas.testing as pdt

cd.get_lat_lon_and_date("ex.gpx")

expected = pd.DataFrame({"lat": [1, 3],
                         "lon": [2, 4],
                         "date": [pd.to_datetime("2022-02-14"),
                                  pd.to_datetime("2020-03-04")]})


def test_get_lat_lon_and_date():
    pdt.assert_frame_equal(expected, cd.get_lat_lon_and_date("ex.gpx"), check_dtype=False)


expected_compass_readings = pd.DataFrame({"date": [pd.to_datetime("2018-05-10"),
                                                   pd.to_datetime("2018-05-11")],
                                          "Bx": [1, 2],
                                          "By": [3, 4]})


def test_read_compass_readings():
    pdt.assert_frame_equal(expected_compass_readings, cd.read_compass_readings("ex.csv"))


expected_data = pd.DataFrame({"lat": [1], "lon": [2], "Bx": [3], "By": [4]})
sample_lat_lon_date = pd.DataFrame({"lat": [1, 2],
                                    "lon": [2, 3],
                                    "date": [pd.to_datetime("2022-04-01"), pd.to_datetime("2022-07-01")]})

sample_compass_readings = pd.DataFrame({"Bx": [3], "By": [4], "date": [pd.to_datetime("2022-04-01")]})


def test_combine_lat_lon_and_date_and_compass_readings():
    pdt.assert_frame_equal(expected_data,
                           cd.combine_lat_lon_and_date_and_compass_readings(sample_lat_lon_date,
                                                                            sample_compass_readings))


sample_readings = pd.DataFrame({'lat': [49.28, 49.26, 49.26],
                                'lon': [123, 123.1, 123.05],
                                "Bx": [1, 2, 3],
                                "BY": [1, 2, 3]})


def test_distance():
    assert round(cd.distance(sample_readings), 6) == 11217.038892
