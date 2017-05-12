import sys
import numpy as np
import requests

from doublegis_api.settings import DoubleGisSettings
from data.models import SubRubric
from utility.safe_request import safe_request


def get_sub_rubrics(region, main_rubrics):
    sub_rubrics = []
    session = requests.Session()
    for main_rubric in main_rubrics:
        r = safe_request(session=session,
                         url=DoubleGisSettings.main_url + '/catalog/rubric/list',
                         params={'key': DoubleGisSettings.key,
                                 'region_id': region.id,
                                 'parent_id': main_rubric.id})
        response = r.json()
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
    return np.unique(sub_rubrics)
