import pickle
import pandas as pd
import numpy as np
import time
import datetime

from data.models import MainRubric, Filial
from doublegis_api.api import Api2Gis
from doublegis_api.clusters_by_polygon import ClusterByPolygon

start_time = time.time()

api = Api2Gis()
api.load()
print('Данные загружены. Регион: {0}'.format(api.region))
print('Количество рубрик: {0}'.format(api.rubrics.shape[0]))
print('Количество смежных рубрик: {0}'.format(api.sub_rubrics.shape[0]))
print('Количество станций метро: {0}'.format(api.metro_stations.shape[0]))
print('Количество зданий: {0}'.format(api.buildings.shape[0]))
print('Количество организаций: {0}'.format(api.organizations.shape[0]))
print('Количество филиалов: {0}'.format(api.filials.shape[0]))

print()
print('Выгрузка новых данных')
new_dataset = api.download_new_data()
print('Количество новых организаций: {0}'.format(new_dataset['new_organizations'].shape[0]))
print('Количество новых филиалов: {0}'.format(new_dataset['new_filials'].shape[0]))
print()
print('Объединение данных')
api.merge_data(new_dataset)

print()
print('Обновление дат всех филиалов')
api.update_filials_dates()

print()
print('Сохранение данных')
api.save()

print()
print('Выгрузка всех ненайденных филиалов')
filials_not_found = []
checked_filials = list(filter(lambda x: x.closed_at_json['2gis_removed_at'] != '', api.filials))
for f in checked_filials:
    org = next(o for o in api.organizations if o.id == f.organization_id)
    filials_not_found.append({'filial_id': str(f.doublegis_id),
                              'organization_id': str(org.id),
                              'name': org.name,
                              'address': '{0}, {1}'.format(f.street_name, f.house),
                              'lon': f.longitude, 'lat': f.latitude,
                              'created_at': f.created_at_json['2gis_appear_at'],
                              'updated_at': f.updated_at_json['2gis_updated_at'],
                              'closed_at_json': f.closed_at_json['2gis_removed_at']})
pd.DataFrame(data=filials_not_found).to_excel('not_found_filials/not_found_filials_at_{0}.xlsx'
                                              .format(datetime.datetime.now().strftime('%Y_%m_%d-%H_%M_%S ')))

elapsed_time = time.time() - start_time
print('Занятое время: {0} секунд'.format(elapsed_time))
