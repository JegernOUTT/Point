import numpy as np
import requests

from doublegis_api.settings import DoubleGisSettings
from data.models import MetroStation


def get_metro_stations(region):
    r = requests.get(DoubleGisSettings.main_url + '/transport/station/list',
                     {'key': DoubleGisSettings.key,
                      'region_id': region.id,
                      'station_type': 'metro',
                      'page_size': 2000,
                      'fields': 'items.platforms.geometry.centroid'})
    if r.status_code == 200:
        response = r.json()
        assert response['result']['total'] > 0
        stations_json = response['result']['items']

        stations = []
        for station in stations_json:
            m = MetroStation()
            m.id = int(station['id'])
            m.name = station['name']
            coordinates = station['platforms'][0]['geometry']['centroid'][6:-1].split()
            m.longitude = float(coordinates[0])
            m.latitude = float(coordinates[1])
            stations.append(m)

        return np.array(stations)
    else:
        raise ConnectionAbortedError()
