import xml.etree.ElementTree as et
from datetime import datetime
import pandas as pd

ns = "{http://www.topografix.com/GPX/1/0}"


def get_lat_lon_and_date(file_name: str) -> pd.DataFrame:
    """
    @param file_name: the name of a xml file containing a collection of observations where each one has a
     latitude, longitude, and the date
    @return: returns a DataFrame where each row contains the latitudes, longitudes, and dates of all the observations
    """
    def lat_lon_date(observation):
        """
        @param observation: an observation containing the latitude, longitude, and date of observation
        @return: a tuple containing the latitude, longitude, and date
        """
        return (float(observation.get("lat")),
                float(observation.get("lon")),
                observation.find('{http://www.topografix.com/GPX/1/0}time').text)

    observations = et.parse(file_name).getroot().iter('{http://www.topografix.com/GPX/1/0}trkpt')
    lat_lon_and_dates_triples = map(lat_lon_date, observations)
    lat, lon, dates = list(zip(*lat_lon_and_dates_triples))
    return pd.DataFrame({"lat": lat, "lon": lon, "date": pd.to_datetime(dates)})


def distance(df: pd.DataFrame) -> int:
    """
    @param df: a DataFrame where each row is an observation containing the latitude, longitude, x-component,
     y-component, and date
    @return: the sum of the distance between each consecutive pair of observations
    """