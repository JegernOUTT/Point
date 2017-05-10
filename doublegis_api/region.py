import requests
import numpy as np

from doublegis_api.settings import DoubleGisSettings
from data.models import Region


def get_region(with_bounds='', region_id=38):
    r = requests.get(DoubleGisSettings.main_url + '/region/get',
                     {'key': DoubleGisSettings.key,
                      'id': region_id,
                      'fields': 'items.name_grammatical_cases,items.domain,'
                                'items.locales,items.time_zone,items.bounds,'
                                'items.statistics,items.locale,items.settlements,'
                                'items.satellites'})
    if r.status_code == 200:
        response = r.json()
        assert response['result']['total'] == 1
        region_json = response['result']['items'][0]

        region = Region()
        region.id = int(region_json['id'])
        region.name = region_json['name']
        region.bounds= [(float(b.split()[1]), float(b.split()[0]))
                        for b in region_json['bounds'][9:-2].split(',')]
        region.org_count = int(region_json['statistics']['org_count'])
        region.branch_count = int(region_json['statistics']['branch_count'])
        region.rubric_count = int(region_json['statistics']['rubric_count'])

        if with_bounds != '':
            region.bounds = np.loadtxt(with_bounds, delimiter=',')[:,[-1, 0]]

        return region
    else:
        raise ConnectionAbortedError()
