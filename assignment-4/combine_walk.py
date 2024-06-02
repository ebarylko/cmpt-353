import pandas as pd
import xml.etree.ElementTree as et


def get_lat_lon_and_date(file_name: str) -> pd.DataFrame:
    """
    @param file_name: the name of a xml file containing a collection of observations where each one has a
     latitude, longitude, and the date
    @return: returns the latitude, longitude, and date of each observation as different columns within a
    DataFrame
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
    return pd.DataFrame({"lat": lat, "lon": lon, "date": pd.to_datetime(dates, utc=True)})


def averages_in_nearest_four_seconds(df: pd.DataFrame) -> pd.DataFrame:
    """
    @param df: a DataFrame with rows containing the time, and other data
    @return: groups the rows to the nearest four seconds and takes the averages of the other data in the groups. Returns
    a DataFrame containing the averages of the grouped rows
    """
    df_cpy = df.copy()
    df_cpy["date"] = df_cpy["date"].dt.round("4s")
    return df_cpy.groupby(["date"]).mean()
