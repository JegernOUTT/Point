import sys
import numpy as np
import requests

from doublegis_api.settings import DoubleGisSettings
from data.models import SubRubric


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


def get_sub_rubrics(region, main_rubrics):
    sub_rubrics = []
    session = requests.Session()
    for main_rubric in main_rubrics:
        r = request(session=session,
                    url=DoubleGisSettings.main_url + '/catalog/rubric/list',
                    params={'key': DoubleGisSettings.key,
                            'region_id': region.id,
                            'parent_id': main_rubric.id})
        response = r.json()
        assert response['result']['total'] > 0
        rubrics_json = response['result']['items']

        for rubric in rubrics_json:
            r = SubRubric()
            r.id = int(rubric['id'])
            r.main_rubric_id = main_rubric.id
            r.name = rubric['name']
            r.alias = rubric['alias']
            r.org_count = int(rubric['org_count'])
            r.branch_count = int(rubric['branch_count'])
            sub_rubrics.append(r)
    return np.array(sub_rubrics)
