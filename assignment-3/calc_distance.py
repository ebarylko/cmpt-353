import xml.etree.ElementTree as et
from datetime import datetime

ns = "{http://www.topografix.com/GPX/1/0}"


def get_lat_lon_and_date(file_name):
    """
    @param file_name: the name of a xml file containing a collection of observations where each one has a
     latitude, longitude, and the date
    @return: returns three collections where the first is the latitudes of all the observations, the second is
    the longitudes of all the observations, and the third is the date of all the observations
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
    lat_lon_and_dates = map(lat_lon_date, observations)
    return list(zip(*lat_lon_and_dates))
