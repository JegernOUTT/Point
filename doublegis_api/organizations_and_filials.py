import requests
import sys
import json
import math
import numpy as np
from multiprocessing.pool import Pool

from doublegis_api.settings import DoubleGisSettings
from data.models import Filial, Organization
from utility.safe_request import safe_request


# Парсинг из json в филиал и организацию
def _parse_organization_and_filial(filial_json, verbose, just_filial=False):
    f = Filial()
    o = Organization()

    try:
        f.doublegis_id = int(filial_json['id'].split('_')[0])
        f.organization_id = int(filial_json['org']['id'])
        f.building_id = int(filial_json['address']['building_id'])
        f.street_name = filial_json['address']['components'][0]['street']
        f.house = filial_json['address']['components'][0]['number']
        f.address_synonyms.append(filial_json['address_name'])
        f.longitude = float(filial_json['point']['lon'])
        f.latitude = float(filial_json['point']['lat'])
        f.created_at_json['2gis_appear_at'] = filial_json['dates']['created_at'] \
            if 'created_at' in filial_json['dates'].keys() else ''
        f.updated_at_json['2gis_updated_at'] = filial_json['dates']['updated_at'] \
            if 'updated_at' in filial_json['dates'].keys() else ''
        f.closed_at_json['2gis_removed_at'] = filial_json['dates']['removed_at'] \
            if 'removed_at' in filial_json['dates'].keys() else ''
    except Exception as e:
        f = None
        if verbose:
            print('Error at filial parsing: {0}'.format(e), file=sys.stderr)

    if just_filial:
        return f

    try:
        o.id = int(filial_json['org']['id'])
        o.name = filial_json['name']
        o.name_primary = filial_json['name_ex']['primary']
        o.name_extension = filial_json['name_ex']['extension'] if 'extension' in filial_json['name_ex'] else ''
        o.name_synonyms.append(o.name)
        o.name_synonyms.append(o.name.lower())
        o.name_synonyms.append(o.name_primary)
        o.name_synonyms.append(o.name_primary.lower())

        if 'rubrics' in filial_json.keys():
            o.main_rubrics['doublegis_rubrics_ids'] = np.unique(np.array([int(r['parent_id']) for r in filial_json['rubrics']]))
            o.sub_rubrics['doublegis_rubrics_ids'] = np.unique(np.array([int(r['id']) for r in filial_json['rubrics']]))

        if 'contact_groups' in filial_json.keys():
            contacts = [c for group in filial_json['contact_groups'] for c in group['contacts']]
            o.contacts_json['email'] += [c['value'] for c in contacts if 'email' == c['type']]
            o.contacts_json['phone'] += [c['value'] for c in contacts if 'phone' == c['type']]
            o.contacts_json['other'] += [c['value'] for c in contacts if 'phone' != c['type'] and 'email' != c['type']]

    except Exception as e:
        o = None
        if verbose:
            print('Error at organization parsing: {0}'.format(e), file=sys.stderr)

    return o, f


# Получение филиалов и организаций по зданиям
def _get_organizations_and_filials_by_buildings(building, session, verbose):
    organizations = []
    filials = []
    r_get_total = safe_request(session=session,
                               url=DoubleGisSettings.main_url + '/catalog/branch/list',
                               params={'key': DoubleGisSettings.key,
                                       'building_id': building.building_id,
                                       'page': 1,
                                       'page_size': 1})
    if r_get_total.json()['meta']['code'] != 200:
        if verbose:
            print('No filials at building {0} or error: {1}'.format(building.building_id,
                                                                    r_get_total.json()),
                  file=sys.stderr)
        return [], []

    else:
        filials_count = int(r_get_total.json()['result']['total'])
        for page_num in range(int(math.ceil(filials_count / 50))):
            r = safe_request(session=session,
                             url=DoubleGisSettings.main_url + '/catalog/branch/list',
                             params={'key': DoubleGisSettings.key,
                                     'building_id': building.building_id,
                                     'page': page_num + 1,
                                     'page_size': 50,
                                     'fields': 'items.region_id,items.point,items.adm_div, '
                                               'items.dates,items.photos,items.see_also,'
                                               'items.locale,items.address,items.schedule,'
                                               'items.name_ex,dym,items.stat,search_attributes,'
                                               'items.reviews,items.ads.options,items.links,'
                                               'items.is_routing_available,items.stop_factors,'
                                               'items.email_for_sending.allowed,items.group,'
                                               'items.external_content,items.contact_groups,'
                                               'items.rubrics,items.flags,filters,widgets,markers,'
                                               'items.attribute_groups,items.reg_bc_url,'
                                               'context_rubrics,request_type,items.org,items.alias'})

            filials_json = r.json()['result']['items']

            for filial_json in filials_json:
                o, f = _parse_organization_and_filial(filial_json, verbose)
                if o is not None:
                    organizations.append(o)
                if f is not None:
                    filials.append(f)

    return organizations, filials


def get_organizations_and_filials_by_buildings(buildings, progress, verbose=False):
    if progress == 'ipython':
        from utility.ipython_utility import log_progress

        organizations = []
        filials = []
        session = requests.Session()
        for building in log_progress(buildings, name='Здания'):
            orgs, fils = _get_organizations_and_filials_by_buildings(building, session, verbose)
            organizations += orgs
            filials += fils

        return np.unique(organizations), np.unique(filials)
    else:
        from tqdm import tqdm

        organizations = []
        filials = []
        session = requests.Session()
        for building in tqdm(buildings):
            orgs, fils = _get_organizations_and_filials_by_buildings(building, session, verbose)
            organizations += orgs
            filials += fils

        return np.unique(organizations), np.unique(filials)


def get_organizations_and_filials_parallel_by_buildings(buildings, workers=15, verbose=False, progress='ipython'):
    all_organizations = np.array([])
    all_filials = np.array([])

    pool = Pool(processes=workers)

    chunk_size = len(buildings) // workers
    results = [pool.apply_async(get_organizations_and_filials_by_buildings, (buildings[chunk_size * i:chunk_size * (i + 1)],
                                                                             progress, verbose))
               for i in range(workers)]

    data = []
    for async in results:
        organizations, filials = async.get()
        data.append((organizations, filials))

    for organizations, filials in data:
        all_organizations = np.append(all_organizations, organizations)
        all_filials = np.append(all_filials, filials)

    return np.unique(all_organizations), np.unique(all_filials)


# Получение филиалов по организациям
def _get_filials_by_organization(organization, session, verbose):
    filials = []
    r_get_total = safe_request(session=session,
                               url=DoubleGisSettings.main_url + '/catalog/branch/list',
                               params={'key': DoubleGisSettings.key,
                                       'org_id': organization.id,
                                       'page': 1,
                                       'page_size': 1})
    if r_get_total.json()['meta']['code'] != 200:
        if verbose:
            print('No filials at organization {0} or error: {1}'.format(organization.id,
                                                                        r_get_total.json()['meta']['code']),
                  file=sys.stderr)
        return []

    filials_count = r_get_total.json()['result']['total']
    for page_num in range(int(math.ceil(filials_count / 50))):
        r = safe_request(session=session,
                         url=DoubleGisSettings.main_url + '/catalog/branch/list',
                         params={'key': DoubleGisSettings.key,
                                 'org_id': organization.id,
                                 'page': page_num + 1,
                                 'page_size': 50,
                                 'fields': 'items.region_id,items.point,items.adm_div, '
                                           'items.dates,items.photos,items.see_also,'
                                           'items.locale,items.address,items.schedule,'
                                           'items.name_ex,dym,items.stat,search_attributes,'
                                           'items.reviews,items.ads.options,items.links,'
                                           'items.is_routing_available,items.stop_factors,'
                                           'items.email_for_sending.allowed,items.group,'
                                           'items.external_content,items.contact_groups,'
                                           'items.rubrics,items.flags,filters,widgets,markers,'
                                           'items.attribute_groups,items.reg_bc_url,'
                                           'context_rubrics,request_type,items.org,items.alias'})

        filials_json = r.json()['result']['items']

        for filial_json in filials_json:
            f = _parse_organization_and_filial(filial_json, verbose, just_filial=True)
            if f is not None:
                filials.append(f)

    return filials


def get_filials_by_organizations(organizations, verbose=False, progress='ipython'):
    if progress == 'ipython':
        from utility.ipython_utility import log_progress

        filials = []
        session = requests.Session()
        for organization in log_progress(organizations, name='Организации'):
            fils = _get_filials_by_organization(organization, session, verbose)
            filials += fils

        return np.unique(filials)

    else:
        from tqdm import tqdm

        filials = []
        session = requests.Session()
        for organization in tqdm(organizations):
            fils = _get_filials_by_organization(organization, session, verbose)
            filials += fils

        return np.unique(filials)


def get_filials_by_organizations_parallel(organizations, workers=15, verbose=False, progress='ipython'):
    all_filials = np.array([])

    pool = Pool(processes=workers)

    chunk_size = len(organizations) // workers
    results = [pool.apply_async(get_filials_by_organizations, (organizations[chunk_size * i:chunk_size * (i + 1)],
                                                               progress, verbose))
               for i in range(workers)]

    data = []
    for async in results:
        filials = async.get()
        data.append(filials)

    for filials in data:
        all_filials = np.append(all_filials, filials)

    return np.unique(all_filials)


# Парсинг из html json файлика, в котором находится филиал / организация
# TODO: Добавить парсинг 200 кодов и обновлять last_update_time
def _parse_by_firm(request, status_code):
    if status_code == 410:
        data = request.text.split('<script type="text/javascript">')[1] \
                           .split('</script>')[0] \
                           .split('stat[pr]\\":10}]":')[1]
        data = data[:data.find(',null,null]') + len(',null,null]')]
        return json.loads(data)[0]['data']
    else:
        raise NotImplementedError()


# Обновление дат филиалов (created_at, removed_at)
def _update_filial_date(filial, session, verbose):
    r = safe_request(session=session,
                     allowed_codes={200, 410, 404},
                     url='https://2gis.ru/spb/firm/{0}'.format(filial.doublegis_id))
    try:
        if r.status_code == 410:
            company = _parse_by_firm(r, status_code=410)

            filial.created_at_json['2gis_appear_at'] = company['dates']['created_at'] \
                if 'created_at' in company['dates'].keys() else ''
            filial.updated_at_json['2gis_updated_at'] = company['dates']['updated_at'] \
                if 'updated_at' in company['dates'].keys() else ''
            filial.closed_at_json['2gis_removed_at'] = company['dates']['deleted_at'] \
                if 'deleted_at' in company['dates'].keys() else ''

        elif r.status_code == 200:
            filial.closed_at_json['2gis_removed_at'] = ''

    except Exception as e:
        if verbose:
            print('Error while company parse {0}. '
                  'Error code: {1}, filial id: {2}'.format(e, r.status_code, filial.doublegis_id))

    return filial


def update_filials_dates(filials, progress='ipython', verbose=False):
    if progress == 'ipython':
        from utility.ipython_utility import log_progress

        updated_filials = []
        session = requests.Session()
        for filial in log_progress(filials, name='Организации'):
            updated_filial = _update_filial_date(filial, session, verbose)
            updated_filials.append(updated_filial)

        return np.unique(updated_filials)

    else:
        from tqdm import tqdm

        updated_filials = []
        session = requests.Session()
        for filial in tqdm(filials):
            updated_filial = _update_filial_date(filial, session, verbose)
            updated_filials.append(updated_filial)

        return np.unique(updated_filials)


def update_filials_dates_parallel(filials, workers=15, verbose=False, progress='ipython'):
    all_filials = np.array([])
    pool = Pool(processes=workers)

    chunk_size = len(filials) // workers
    results = [pool.apply_async(update_filials_dates, (filials[chunk_size * i:chunk_size * (i + 1)],
                                                       progress, verbose))
               for i in range(workers)]

    data = []
    for async in results:
        data.append(async.get())
    for filials in data:
        all_filials = np.append(all_filials, filials)
    return np.unique(all_filials)


# Получение удалённых филиалов / организаций по идентификаторам
def _get_removed_organization_and_filial_by_ids(i, session, verbose):
    r = safe_request(session=session,
                     allowed_codes={200, 410, 404},
                     url='https://2gis.ru/spb/firm/{0}'.format(i))

    try:
        if r.status_code == 410:
            company = _parse_by_firm(r, status_code=410)

            if 'region_id' not in company.keys() \
                    or int(company['region_id']) != 38:
                return None, None

            return _parse_organization_and_filial(company, verbose)
    except Exception as e:
        if verbose:
            print('Error while company parse {0}. Data: {1}. Status code: {2}'.format(e, i, r.status_code))
        return None, None


def _get_removed_organizations_and_filials_by_ids(ids, progress, verbose=False):
    if progress == 'ipython':
        from utility.ipython_utility import log_progress

        organizations = []
        filials = []

        session = requests.Session()
        for i in log_progress(ids, name='Идентификатор'):
            o, f = _get_removed_organization_and_filial_by_ids(i, session, verbose)
            if o is not None:
                organizations.append(o)
            if f is not None:
                filials.append(f)
        return np.unique(filials), np.unique(organizations)
    else:
        from tqdm import tqdm

        organizations = []
        filials = []

        session = requests.Session()
        for i in tqdm(ids):
            o, f = _get_removed_organization_and_filial_by_ids(i, session, verbose)
            if o is not None:
                organizations.append(o)
            if f is not None:
                filials.append(f)
        return filials, organizations


def _get_removed_organizations_and_filials_parallel_by_ids(ids, workers=15, verbose=False, progress='ipython'):
    filials = []
    organizations = []

    pool = Pool(processes=workers)
    chunk_size = len(ids) // workers
    results = [pool.apply_async(update_filials_dates, (ids[chunk_size * i:chunk_size * (i + 1)],
                                                       progress, verbose))
               for i in range(workers)]

    data = []
    for async in results:
        data.append(async.get())

    for fil, org in data:
        filials += fil
        organizations += org

    return np.unique(filials), np.unique(organizations)