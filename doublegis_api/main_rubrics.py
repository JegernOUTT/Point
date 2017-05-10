import numpy as np
import requests
import sys

from doublegis_api.settings import DoubleGisSettings
from data.models import MainRubric


def request(session, **kwargs):
    while True:
        try:
            r = session.get(**kwargs, timeout=3)
            if r.status_code == 200:
                return r
            else:
                print('Error in request: {0}'.format('Status code == ' + r.status_code),
                      file=sys.stderr)
                continue
        except Exception as e:
            print('Error in request: {0}'.format(e), file=sys.stderr)
            continue


def get_main_rubrics(region):
    session = requests.Session()
    r = request(session=session,
                url=DoubleGisSettings.main_url + '/catalog/rubric/list',
                params={'key': DoubleGisSettings.key,
                        'region_id': region.id})
    response = r.json()
    assert response['result']['total'] > 0
    rubrics_json = response['result']['items']

    rubrics = []
    for rubric in rubrics_json:
        r = MainRubric()
        r.id = int(rubric['id'])
        r.name = rubric['name']
        r.alias = rubric['alias']
        r.org_count = int(rubric['org_count'])
        r.branch_count = int(rubric['branch_count'])
        rubrics.append(r)

        return np.array(rubrics)
