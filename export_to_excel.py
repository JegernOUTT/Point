import time

from doublegis_api.api import Api2Gis
from export.excel_export import export_excel
from filtering.categories import get_filter_cats_ids, filter_by_categories

start_time = time.time()

print()
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
print('Фильтрация филиалов')
print('Количество филиалов до фильтрации: {0}'.format(len(api.filials)))
main, sub = get_filter_cats_ids(api.rubrics, api.sub_rubrics)
print('Для фильтрации найдены {0} категорий и {1} подкатегорий'.format(len(main), len(sub)))
api.filials = filter_by_categories(api.filials, api.organizations, main, sub, progress='console')
print('Количество филиалов после фильтрации: {0}'.format(len(api.filials)))

print()
print('Экспорт филиалов')
export_excel(api, progress='console')
print('Экспорт завершён')

elapsed_time = time.time() - start_time
print('Занятое время: {0} секунд'.format(elapsed_time))
