import os
from datetime import datetime

import dateutil.parser
import pandas as pd

from doublegis_api.metro_coordinates import get_nearest_stations
from filtering.categories import get_data_by_filial


def _process_element(api, filial):
    try:
        filial, org, main_rubric, sub_rubrics = get_data_by_filial(api, filial)
    except:
        return None
    metro, distance = get_nearest_stations(filial, api.metro_stations)[0]
    if distance > 1000:
        return None

    living_period = None
    if filial.created_at_json['2gis_appear_at'] != '':
        created_at = dateutil.parser.parse(filial.created_at_json['2gis_appear_at']).replace(tzinfo=None)
        if filial.closed_at_json['2gis_removed_at'] != '':
            removed_at = dateutil.parser.parse(filial.closed_at_json['2gis_removed_at']).replace(tzinfo=None)
            living_period = (removed_at - created_at).days // 31
        else:
            living_period = (datetime.today() - created_at).days // 31

    return [main_rubric.name, org.name_primary, filial.address_synonyms[0], filial.latitude,
            filial.longitude, metro.name, metro.latitude, metro.longitude, distance / 1000.,
            filial.created_at_json['2gis_appear_at'], filial.closed_at_json['2gis_removed_at'],
            living_period, 1 if filial.closed_at_json['2gis_removed_at'] == '' else 0]


def export_excel(api, progress='ipython'):
    if progress == 'ipython':
        from utility.ipython_utility import log_progress

        data = []
        for filial in log_progress(api.filials, name='Филиалы'):
            row = _process_element(api, filial)
            if row is not None:
                data.append(row)

        df = pd.DataFrame(data=data,
                            columns=['category', 'company', 'address', 'latitude', 'longtitude', 'nearest_name',
                                     'nearest_metro_lat', 'nearest_metro_long', 'nearest_distance',
                                     'created_date', 'removed_date', 'living_period', 'current_status']) \
            .rename(columns={'category': 'Отрасль',
                             'company': 'Компания',
                             'address': 'Адрес',
                             'latitude': 'Широта_к',
                             'longtitude': 'Долгота_к',
                             'nearest_name': 'Ближайшее метро',
                             'nearest_metro_lat': 'Широта_м',
                             'nearest_metro_long': 'Долгота_м',
                             'nearest_distance': 'Расстояние до метро',
                             'created_date': 'Дата открытия',
                             'removed_date': 'Дата закрытия',
                             'living_period': 'Срок жизни, мес.',
                             'current_status': 'Статус'})
        if not os.path.exists('data/files/excel_exported/'):
            os.makedirs('data/files/excel_exported/')
        df.to_excel('data/files/excel_exported/exported_{0}.xlsx'.format(datetime.now().strftime('%Y_%m_%d-%H_%M_%S ')))
        return df
    else:
        from tqdm import tqdm

        data = []
        for filial in tqdm(api.filials):
            row = _process_element(api, filial)
            if row is not None:
                data.append(row)

        df = pd.DataFrame(data=data,
                            columns=['category', 'company', 'address', 'latitude', 'longtitude', 'nearest_name',
                                     'nearest_metro_lat', 'nearest_metro_long', 'nearest_distance',
                                     'created_date', 'removed_date', 'living_period', 'current_status']) \
            .rename(columns={'category': 'Отрасль',
                             'company': 'Компания',
                             'address': 'Адрес',
                             'latitude': 'Широта_к',
                             'longtitude': 'Долгота_к',
                             'nearest_name': 'Ближайшее метро',
                             'nearest_metro_lat': 'Широта_м',
                             'nearest_metro_long': 'Долгота_м',
                             'nearest_distance': 'Расстояние до метро',
                             'created_date': 'Дата открытия',
                             'removed_date': 'Дата закрытия',
                             'living_period': 'Срок жизни, мес.',
                             'current_status': 'Статус'})
        if not os.path.exists('data/files/excel_exported/'):
            os.makedirs('data/files/excel_exported/')
        df.to_excel('data/files/excel_exported/exported_{0}.xlsx'.format(datetime.now().strftime('%Y_%m_%d-%H_%M_%S ')))
        return df

