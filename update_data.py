import datetime
import os
import time

import pandas as pd

from doublegis_api.api import Api2Gis

start_time = time.time()

api = Api2Gis()
api.load()
api.describe()

print()
print('Выгрузка новых данных')
orgs, fils = api.download_new_data(progress='console')
print('Количество новых организаций: {0}'.format(len(orgs)))
print('Количество новых филиалов: {0}'.format(len(fils)))

print()
print('Объединение данных')
api.merge_data(organizations=orgs, filials=fils)

print()
print('Обновление дат всех филиалов')
api.update_filials_dates(progress='console')

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
if not os.path.exists('data/files/not_found_filials/'):
    os.makedirs('data/files/not_found_filials/')
pd.DataFrame(data=filials_not_found).to_excel('data/files/not_found_filials/not_found_filials_at_{0}.xlsx'
                                              .format(datetime.datetime.now().strftime('%Y_%m_%d-%H_%M_%S ')))

elapsed_time = time.time() - start_time
print('Занятое время: {0} секунд'.format(elapsed_time))
