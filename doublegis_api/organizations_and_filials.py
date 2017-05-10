import requests
import sys
import json
import math
import numpy as np
from multiprocessing.pool import ThreadPool, Pool

from doublegis_api.settings import DoubleGisSettings
from data.models import Filial, Organization


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


def get_organizations_and_filials(buildings, verbose=False):
    organizations = []
    filials = []
    organizations_ids = set()

    session = requests.Session()
    for building in buildings:
        r_get_total = request(session=session,
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
            continue

        filials_count = r_get_total.json()['result']['total']
        for page_num in range(int(math.ceil(filials_count / 50))):
            r = request(session=session,
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

            for filial in filials_json:

                f = Filial()
                try:
                    f.doublegis_id = int(filial['id'].split('_')[0])
                    f.organization_id = int(filial['org']['id'])
                    f.building_id = int(building.building_id)
                    f.street_name = filial['address']['components'][0]['street']
                    f.house = filial['address']['components'][0]['number']
                    f.address_synonyms.append(filial['address_name'])
                    f.longitude = float(filial['point']['lon'])
                    f.latitude = float(filial['point']['lat'])
                    f.created_at_json['2gis_appear_at'] = filial['dates']['created_at'] if 'created_at' in filial['dates'].keys() else ''
                    f.updated_at_json['2gis_updated_at'] = filial['dates']['updated_at'] if 'updated_at' in filial['dates'].keys() else ''
                    f.closed_at_json['2gis_removed_at'] = filial['dates']['removed_at'] if 'removed_at' in filial['dates'].keys() else ''

                    if f.organization_id not in organizations_ids:
                        o = Organization()
                        o.id = int(filial['org']['id'])
                        o.name = filial['name']
                        o.name_primary = filial['name_ex']['primary']
                        o.name_extension = filial['name_ex']['extension'] if 'extension' in filial['name_ex'] else ''
                        o.name_synonyms.append(o.name)
                        o.name_synonyms.append(o.name.lower())
                        o.name_synonyms.append(o.name_primary)
                        o.name_synonyms.append(o.name_primary.lower())
                        o.main_rubrics['doublegis_rubrics_ids'] = np.unique(np.array([r['parent_id'] for r in filial['rubrics']]))
                        o.sub_rubrics['doublegis_rubrics_ids'] = np.unique(np.array([r['id'] for r in filial['rubrics']]))

                        contacts = [c for group in filial['contact_groups'] for c in group['contacts']]
                        o.contacts_json['email'] += [c['value'] for c in contacts if 'email' == c['type']]
                        o.contacts_json['phone'] += [c['value'] for c in contacts if 'phone' == c['type']]
                        o.contacts_json['other'] += [c['value'] for c in contacts if 'phone' != c['type'] and 'email' != c['type']]

                        organizations.append(o)
                        organizations_ids.add(o.id)

                        if verbose:
                            print('Organization added: {0}'.format(o))

                except Exception as e:
                    print('Error at filial parsing: {0}', e, file=sys.stderr)
                    continue

                filials.append(f)

                if verbose:
                    print('Filial added: {0}'.format(f))

    return np.unique(organizations), np.unique(filials)


def get_filials_by_organizations(organizations, verbose=False):
    filials = []
    session = requests.Session()

    for org in organizations:
        r_get_total = request(session=session,
                              url=DoubleGisSettings.main_url + '/catalog/branch/list',
                              params={'key': DoubleGisSettings.key,
                                      'org_id': org.id,
                                      'page': 1,
                                      'page_size': 1})
        if r_get_total.json()['meta']['code'] != 200:
            if verbose:
                print('No filials at organization {0} or error: {1}'.format(org.id, r_get_total.json()),
                      file=sys.stderr)
            continue

        filials_count = r_get_total.json()['result']['total']
        for page_num in range(int(math.ceil(filials_count / 50))):
            r = request(session=session,
                        url=DoubleGisSettings.main_url + '/catalog/branch/list',
                        params={'key': DoubleGisSettings.key,
                                'org_id': org.id,
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

            for filial in filials_json:
                f = Filial()
                try:
                    f.doublegis_id = int(filial['id'].split('_')[0])
                    f.organization_id = int(filial['org']['id'])
                    f.building_id = int(filial['address']['building_id'])
                    f.street_name = filial['address']['components'][0]['street']
                    f.house = filial['address']['components'][0]['number']
                    f.address_synonyms.append(filial['address_name'])
                    f.longitude = float(filial['point']['lon'])
                    f.latitude = float(filial['point']['lat'])
                    f.created_at_json['2gis_appear_at'] = filial['dates']['created_at'] if 'created_at' in filial[
                        'dates'].keys() else ''
                    f.updated_at_json['2gis_updated_at'] = filial['dates']['updated_at'] if 'updated_at' in filial[
                        'dates'].keys() else ''
                    f.closed_at_json['2gis_removed_at'] = filial['dates']['removed_at'] if 'removed_at' in filial[
                        'dates'].keys() else ''

                except Exception as e:
                    print('Error at filial parsing: {0}'.format(filials_json), e, file=sys.stderr)
                    continue

                filials.append(f)

                if verbose:
                    print('Filial added: {0}'.format(f))

    return np.unique(filials)


def modded_request(session, allowed_errors, **kwargs):
    while True:
        try:
            r = session.get(**kwargs, timeout=3)
            if r.status_code in allowed_errors:
                return r
            else:
                # print('Error in request: {0}'.format('Status code == ' + r.status_code),
                #       file=sys.stderr)
                continue
        except Exception as e:
            # print('Error in request: {0}'.format(e), file=sys.stderr)
            continue


def update_filials_dates(filials, verbose=False):
    session = requests.Session()
    for f in filials:
        r = modded_request(session=session,
                           allowed_errors={200, 410, 404},
                           url='https://2gis.ru/spb/firm/{0}'.format(f.doublegis_id))

        try:
            if r.status_code == 410:
                data = r.text.split('<script type="text/javascript">')[1] \
                             .split('</script>')[0] \
                             .split('stat[pr]\\":10}]":')[1]
                data = data[:data.find(',null,null]') + len(',null,null]')]
                company = json.loads(data)[0]['data']

                f.created_at_json['2gis_appear_at'] = company['dates']['created_at'] \
                    if 'created_at' in company['dates'].keys() else ''
                f.updated_at_json['2gis_updated_at'] = company['dates']['updated_at'] \
                    if 'updated_at' in company['dates'].keys() else ''
                f.closed_at_json['2gis_removed_at'] = company['dates']['deleted_at'] \
                    if 'deleted_at' in company['dates'].keys() else ''
            elif r.status_code == 200:
                f.closed_at_json['2gis_removed_at'] = ''

        except Exception as e:
            print('Error while company parse {0}. Error code: {1}, filial id: {2}'.format(e, r.status_code,
                                                                                          f.doublegis_id))
    return filials
            

def update_filials_dates_parallel(workers, filials, verbose=False):
    all_filials = np.array([])
    pool = Pool(processes=workers)

    chunk_size = len(filials) // workers
    results = [pool.apply_async(update_filials_dates, (filials[chunk_size * i:chunk_size * (i + 1)],
                                                       verbose))
               for i in range(workers)]

    data = []
    for async in results:
        data.append(async.get())
    for filials in data:
        all_filials = np.append(all_filials, filials)
    return np.unique(all_filials)



def get_filials_by_organizations_parallel(workers, organizations, verbose=False):
    all_filials = np.array([])

    pool = Pool(processes=workers)

    chunk_size = len(organizations) // workers
    results = [pool.apply_async(get_filials_by_organizations, (organizations[chunk_size * i:chunk_size * (i + 1)],
                                                               verbose))
               for i in range(workers)]

    data = []
    for async in results:
        filials = async.get()
        data.append(filials)

    for filials in data:
        all_filials = np.append(all_filials, filials)

    return np.unique(all_filials)


def get_organizations_and_filials_parallel(workers, buildings, verbose=False):
    all_organizations = np.array([])
    all_filials = np.array([])

    pool = Pool(processes=workers)

    chunk_size = len(buildings) // workers
    results = [pool.apply_async(get_organizations_and_filials, (buildings[chunk_size * i:chunk_size * (i + 1)],
                                                                verbose))
               for i in range(workers)]

    data = []
    for async in results:
        organizations, filials = async.get()
        data.append((organizations, filials))

    for organizations, filials in data:
        all_organizations = np.append(all_organizations, organizations)
        all_filials = np.append(all_filials, filials)

    return np.unique(all_organizations), np.unique(all_filials)
