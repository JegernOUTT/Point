import numpy as np

import requests
import sys
from multiprocessing.pool import Pool

from doublegis_api.settings import DoubleGisSettings
from data.models import Building
from utility.safe_request import safe_request


def _get_building(cluster, session, radius_meters, verbose):
    r = safe_request(session=session,
                     url=DoubleGisSettings.main_url + '/geo/search',
                     params={'key': DoubleGisSettings.key,
                             'point': '{0},{1}'.format(cluster[0], cluster[1]),
                             'type': 'building',
                             'radius': radius_meters,
                             'page_size': 2000,
                             'fields': 'items.geometry.centroid,items.adm_div,'
                                       'items.address,items.floors,items.attraction,'
                                       'items.statistics,items.level_count,items.capacity,'
                                       'items.description,items.context,items.access_name,'
                                       'items.is_paid,items.access,items.access_comment,'
                                       'items.schedule'})

    response = r.json()
    if response['meta']['code'] == 200:
        buildings_json = response['result']['items']

        count = 0
        for building in buildings_json:
            b = Building()
            try:
                b.building_id = int(building['id'])
                b.address_synonyms.append(building['address_name'])
                b.street_name = building['address']['components'][0]['street']
                b.street_id = building['address']['components'][0]['street_id']
                b.house = building['address']['components'][0]['number']
                b.floors = building['floors']['ground_count']
                coordinates = building['geometry']['centroid'][6:-1].split()
                b.longitude = float(coordinates[0])
                b.latitude = float(coordinates[1])
            except Exception as e:
                if verbose:
                    print('{0}, {1}: '.format(cluster[0], cluster[1]),
                          'Not valid: {0}. Error: {1}'.format(building['purpose_name'], e),
                          file=sys.stderr)
                return None

            if verbose:
                print('{0}, {1}: '.format(cluster[0], cluster[1]), b)

            count += 1
            return b
    else:
        if verbose:
            print('{0}, {1}: '.format(cluster[0], cluster[1]),
                  'Not found',
                  file=sys.stderr)
        return None


def get_buildings_by_radius(clusters, radius_meters=250, verbose=False, progress='ipython'):
    if progress == 'ipython':
        from utility.ipython_utility import log_progress

        buildings = []
        session = requests.Session()
        for cluster in log_progress(clusters, name='Кластеры'):
            b = _get_building(cluster, session, radius_meters, verbose)
            if b is not None:
                buildings.append(b)
        return buildings
    else:
        from tqdm import tqdm

        buildings = []
        session = requests.Session()
        for cluster in tqdm(clusters):
            b = _get_building(cluster, session, radius_meters, verbose)
            if b is not None:
                buildings.append(b)
        return buildings


def get_buildings_by_radius_parallel(clusters, workers=15, radius_meters=250, verbose=False, progress='ipython'):
    all_buildings = []

    pool = Pool(processes=workers)

    chunk_size = len(clusters) // workers
    results = [pool.apply_async(get_buildings_by_radius, (clusters[chunk_size * i:chunk_size * (i + 1)],
                                                          radius_meters, verbose, progress))
               for i in range(workers)]

    for async in results:
        buildings = async.get()
        all_buildings += buildings
    return np.unique(all_buildings)
