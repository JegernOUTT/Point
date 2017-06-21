import os
import sys
import pickle

import numpy as np

from doublegis_api.building import get_buildings_by_radius_parallel
from doublegis_api.clusters_by_polygon import ClusterByPolygon
from doublegis_api.main_rubrics import get_main_rubrics
from doublegis_api.metro_coordinates import add_metro_distances
from doublegis_api.metro_stations import get_metro_stations
from doublegis_api.organizations_and_filials import \
    get_filials_by_organizations_parallel, update_filials_dates_parallel, \
    get_organizations_and_filials_parallel_by_buildings, _get_removed_organizations_and_filials_parallel_by_ids
from doublegis_api.region import get_region
from doublegis_api.sub_rubrics import get_sub_rubrics


class Api2Gis:
    def __init__(self):
        self.region = None
        self.rubrics = None
        self.sub_rubrics = None
        self.metro_stations = None
        self.buildings = None
        self.organizations = None
        self.filials = None

    def download_data(self, progress, verbose=False):
        print('Загрузка региона')
        self.region = get_region(with_bounds='data/files/2gis/bounds.txt')
        cluster = ClusterByPolygon(region=self.region, mode='file', radius_meters=250)

        print('Загрузка рубрик')
        self.rubrics = get_main_rubrics(self.region)
        print('Количество рубрик: {0}'.format(self.rubrics.shape[0]))

        print('Загрузка подрубрик')
        self.sub_rubrics = get_sub_rubrics(self.region, self.rubrics)
        print('Количество подрубрик: {0}'.format(self.sub_rubrics.shape[0]))

        print('Загрузка станций метро')
        self.metro_stations = get_metro_stations(self.region)
        print('Количество станций метро: {0}'.format(self.metro_stations.shape[0]))

        print('Загрузка зданий')
        self.buildings = get_buildings_by_radius_parallel(clusters=cluster.cluster_centers,
                                                          progress=progress, verbose=verbose)
        print('Количество зданий: {0}'.format(self.buildings.shape[0]))

        print('Загрузка организаций и филиалов')
        self.organizations, self.filials = get_organizations_and_filials_parallel_by_buildings(buildings=self.buildings,
                                                                                               progress=progress,
                                                                                               verbose=verbose)
        print('Количество организаций: {0}. Количество филиалов: {1}'.format(self.organizations.shape[0],
                                                                             self.filials.shape[0]))

    def save(self, path='data/files/2gis/', filename='data.pickle'):
        if not os.path.exists(path):
            os.makedirs(path)

        with open(path + filename, 'wb') as file:
            pickle.dump({'region': self.region,
                         'rubrics': self.rubrics,
                         'sub_rubrics': self.sub_rubrics,
                         'metro_stations': self.metro_stations,
                         'buildings': self.buildings,
                         'organizations': self.organizations,
                         'filials': self.filials}, file)

    def load(self, path='data/files/2gis/', filename='data.pickle'):
        with open(path + filename, 'rb') as f:
            data = pickle.load(f)
            self.region = data['region']
            self.rubrics = data['rubrics']
            self.sub_rubrics = data['sub_rubrics']
            self.metro_stations = data['metro_stations']
            self.buildings = data['buildings']
            self.organizations = data['organizations']
            self.filials = data['filials']

    def add_metro_distances(self):
        self.filials = np.array(list(map(lambda x: add_metro_distances(x, self.metro_stations), self.filials)))

    def describe(self, file=sys.stdout):
        print('Регион: {0}'.format(self.region.name), file=file)
        print('Количество рубрик: {0}'.format(len(self.rubrics)), file=file)
        print('Количество смежных рубрик: {0}'.format(len(self.sub_rubrics)), file=file)
        print('Количество станций метро: {0}'.format(len(self.metro_stations)), file=file)
        print('Количество зданий: {0}'.format(len(self.buildings)), file=file)
        print('Количество организаций: {0}'.format(len(self.organizations)), file=file)
        print('Количество филиалов: {0}'.format(len(self.filials)), file=file)

    def download_new_data(self, progress,
                          bounds_filename='data/files/2gis/bounds.txt',
                          verbose=False):
        print('Загрузка региона')
        self.region = get_region(with_bounds=bounds_filename)
        cluster = ClusterByPolygon(region=self.region, mode='file', radius_meters=250)

        print('Загрузка рубрик')
        rubrics = get_main_rubrics(region=self.region)

        print('Загрузка подрубрик')
        sub_rubrics = get_sub_rubrics(region=self.region, main_rubrics=rubrics)

        print('Загрузка станций метро')
        metro_stations = get_metro_stations(region=self.region)

        print('Загрузка зданий')
        buildings = get_buildings_by_radius_parallel(clusters=cluster.cluster_centers,
                                                     progress=progress, verbose=verbose)

        print('Загрузка организаций и филиалов')
        organizations, filials = get_organizations_and_filials_parallel_by_buildings(buildings=buildings,
                                                                                     progress=progress,
                                                                                     verbose=verbose)

        print('Загрузки завершены. Обработка загруженных данных')
        # Override region,
        # Union for rubrics, sub_rubrics, buildings, metro_stations
        # Returning difference for organizations, filials
        self.metro_stations = np.union1d(self.metro_stations, metro_stations)
        self.rubrics = np.union1d(self.rubrics, rubrics)
        self.sub_rubrics = np.union1d(self.sub_rubrics, sub_rubrics)
        self.buildings = np.union1d(self.buildings, buildings)

        new_organizations = np.setdiff1d(organizations, self.organizations)
        new_filials = np.setdiff1d(filials, self.filials)

        return new_organizations, new_filials

    def merge_data(self, organizations, filials):
        self.organizations = np.unique(np.append(self.organizations, organizations))
        self.filials = np.unique(np.append(self.filials, filials))

    # Запрос временно не нужен, так как есть более удобный способ через "чёрный вход" 2ГИС
    def find_removed_filials(self, progress, verbose=False):
        not_found_filials = list(filter(lambda x: x.closed_at_json['2gis_removed_at'] != '', self.filials))
        organizations = list(map(lambda x: next(org for org in self.organizations if org.id == x.organization_id),
                                 not_found_filials))
        print('Вы имеете {0} удалённых филиалов до обновления'.format(len(not_found_filials)))

        loaded_filials = get_filials_by_organizations_parallel(workers=15,
                                                               progress=progress,
                                                               organizations=np.unique(organizations))

        found = []
        not_found = []
        for f in not_found_filials:
            if f in loaded_filials:
                f.closed_at_json['2gis_removed_at'] = ''
                found.append(f)
                if verbose:
                    print('Найден закрывшийся ранее филиал: {0}', f)
            else:
                # Здесь можно добавить дату закрытия
                not_found.append(f)

        return {'found': np.array(found), 'not_found': np.array(not_found)}

    def update_filials_dates(self, progress, verbose=True):
        if verbose:
            before_filials = list(filter(lambda x: x.closed_at_json['2gis_removed_at'] != '', self.filials))
            print('До обновления имеется {0} удалённых филиалов'.format(len(before_filials)))

        self.filials = update_filials_dates_parallel(filials=self.filials, progress=progress)

        if verbose:
            after_filials = list(filter(lambda x: x.closed_at_json['2gis_removed_at'] != '', self.filials))
            print('После обновления имеется {0} удалённых филиалов'.format(len(after_filials)))

    def get_removed_orgs_by_ids(self, ids, progress, verbose=True):
        if verbose:
            print('До обновления имеется {0} филиалов'.format(len(self.filials)))
            before_filials = list(filter(lambda x: x.closed_at_json['2gis_removed_at'] != '', self.filials))
            print('До обновления имеется {0} удалённых филиалов'.format(len(before_filials)))

        new_filials, new_organizations = _get_removed_organizations_and_filials_parallel_by_ids(ids=ids,
                                                                                                progress=progress)

        self.filials = np.union1d(self.filials, new_filials)
        self.organizations = np.union1d(self.organizations, new_organizations)

        if verbose:
            print('После обновления имеется {0} филиалов'.format(len(self.filials)))
            after_filials = list(filter(lambda x: x.closed_at_json['2gis_removed_at'] != '', self.filials))
            print('После обновления имеется {0} удалённых филиалов'.format(len(after_filials)))
