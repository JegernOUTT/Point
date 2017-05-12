import numpy as np
import requests

from doublegis_api.settings import DoubleGisSettings
from data.models import MainRubric
from utility.safe_request import safe_request


def get_main_rubrics(region):
    session = requests.Session()
    r = safe_request(session=session,
                     url=DoubleGisSettings.main_url + '/catalog/rubric/list',
                     params={'key': DoubleGisSettings.key,
                             'region_id': region.id})
    response = r.json()
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
    return np.unique(rubrics)
