import requests
import numpy as np
import html5lib

from utility.safe_request import safe_request


class TheProperty_Ru:
    def __init__(self, city_name='sankt-peterburg'):
        self.city_name = city_name
        self.url = 'http://theproperty.ru'

    def load_raw_estates(self):
        session = requests.Session()

        def add_prop_type(prop_type, x):
            x['prop_type'] = prop_type
            return x

        data = []
        for prop_type in ['office', 'torgovye', 'kafe', 'warehouse', 'building']:
            for i in range(1, 9999):
                r = safe_request(session=session,
                                 url='{0}/{1}/lease/{2}/'.format(self.url,
                                                                 self.city_name,
                                                                 prop_type),
                                 params={'view': 'table',
                                         'page': i,
                                         'ajaxForYandexMap': 1,
                                         'perPage': 5000})
                if len(r.json()['features']) > 0:
                    data += list(map(lambda x: add_prop_type(prop_type, x), r.json()['features']))
                else:
                    break
                print('Add {} page: {}'.format(prop_type, i))

        return data

    def load_whole_information(self, raw_estates):
        session = requests.Session()

        for estate in raw_estates:
            r = safe_request(session=session,
                             url='{0}/{1}'.format(self.url, estate['id']))
            parsed = html5lib.parse(r.text, r.encoding)

