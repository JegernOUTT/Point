import numpy as np
import pickle

from doublegis_api.building import get_buildings_by_radius_parallel
from doublegis_api.clusters_by_polygon import ClusterByPolygon
from doublegis_api.main_rubrics import get_main_rubrics
from doublegis_api.metro_stations import get_metro_stations
from doublegis_api.organizations_and_filials import get_organizations_and_filials_parallel, \
    get_filials_by_organizations, get_filials_by_organizations_parallel, update_filials_dates_parallel
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

    def download_data(self, verbose=False):
        print('Loading region')
        self.region = get_region(with_bounds='doublegis_api/data/bounds.txt')
        cluster = ClusterByPolygon(region=self.region, mode='file', radius_meters=250)

        print('Loading rubrics')
        self.rubrics = get_main_rubrics(self.region)
        print('Rubrics count: {0}'.format(self.rubrics.shape[0]))

        print('Loading sub-rubrics')
        self.sub_rubrics = get_sub_rubrics(self.region, self.rubrics)
        print('Sub-rubrics count: {0}'.format(self.sub_rubrics.shape[0]))

        print('Loading metro stations')
        self.metro_stations = get_metro_stations(self.region)
        print('Metro stations count: {0}'.format(self.metro_stations.shape[0]))

        print('Loading buildings')
        self.buildings = get_buildings_by_radius_parallel(workers=15, radius_meters=250,
                                                          clusters=cluster.cluster_centers, verbose=verbose)
        print('Buildings count: {0}'.format(self.buildings.shape[0]))

        print('Loading organizations and filials')
        self.organizations, self.filials = get_organizations_and_filials_parallel(workers=15, buildings=self.buildings,
                                                                                  verbose=verbose)
        print('Organizations count: {0}. Filials count: {1}'.format(self.organizations.shape[0],
                                                                    self.filials.shape[0]))

    def save(self, filename='doublegis_api/data/data.pickle'):
        with open(filename, 'wb') as file:
            pickle.dump({'region': self.region,
                         'rubrics': self.rubrics,
                         'sub_rubrics': self.sub_rubrics,
                         'metro_stations': self.metro_stations,
                         'buildings': self.buildings,
                         'organizations': self.organizations,
                         'filials': self.filials}, file)

    def load(self, filename='doublegis_api/data/data.pickle'):
        with open(filename, 'rb') as f:
            data = pickle.load(f)
            self.region = data['region']
            self.rubrics = data['rubrics']
            self.sub_rubrics = data['sub_rubrics']
            self.metro_stations = data['metro_stations']
            self.buildings = data['buildings']
            self.organizations = data['organizations']
            self.filials = data['filials']

    def download_new_data(self, verbose=False):
        self.region = get_region(with_bounds='doublegis_api/data/bounds.txt')
        cluster = ClusterByPolygon(region=self.region, mode='file', radius_meters=250)
        rubrics = get_main_rubrics(self.region)
        sub_rubrics = get_sub_rubrics(self.region, rubrics)
        self.metro_stations = get_metro_stations(self.region)

        buildings = get_buildings_by_radius_parallel(workers=15, radius_meters=250,
                                                     clusters=cluster.cluster_centers, verbose=verbose)

        organizations, filials = get_organizations_and_filials_parallel(workers=15, buildings=buildings,
                                                                        verbose=verbose)
        # Override region, metro_stations
        # Union for rubrics, sub_rubrics, buildings
        # Returning difference for organizations, filials
        self.rubrics = np.union1d(self.rubrics, rubrics)
        self.sub_rubrics = np.union1d(self.sub_rubrics, sub_rubrics)
        self.buildings = np.union1d(self.buildings, buildings)

        new_organizations = np.setdiff1d(organizations, self.organizations)
        new_filials = np.setdiff1d(filials, self.filials)

        return {'new_organizations': new_organizations, 'new_filials': new_filials}

    def merge_data(self, data):
        new_organizations = data['new_organizations']
        new_filials = data['new_filials']

        # Add new data
        self.organizations = np.unique(np.append(self.organizations, new_organizations))
        self.filials = np.unique(np.append(self.filials, new_filials))

    def find_removed_filials(self, verbose=False):
        not_found_filials = list(filter(lambda x: x.closed_at_json['2gis_removed_at'] != '', self.filials))
        organizations = list(map(lambda x: next(org for org in self.organizations if org.id == x.organization_id),
                                 not_found_filials))
        print('You have {0} not found filials'.format(len(not_found_filials)))

        loaded_filials = get_filials_by_organizations_parallel(workers=15, organizations=np.unique(organizations))

        found = []
        not_found = []
        for f in not_found_filials:
            if f in loaded_filials:
                f.closed_at_json['2gis_removed_at'] = ''
                found.append(f)
                if verbose:
                    print('Found disappeared filial by id: {0}', f)
            else:
                not_found.append(f)
                if verbose:
                    print('Disappeared filial not found by id: {0}', f)

        return {'found': np.array(found), 'not_found': np.array(not_found)}

    def update_filials_dates(self):
        before_filials = list(filter(lambda x: x.closed_at_json['2gis_removed_at'] != '', self.filials))
        print('You have {0} not found filials before updating'.format(len(before_filials)))

        self.filials = update_filials_dates_parallel(workers=15, filials=self.filials)

        after_filials = list(filter(lambda x: x.closed_at_json['2gis_removed_at'] != '', self.filials))
        print('You have {0} not found filials after updating'.format(len(after_filials)))
