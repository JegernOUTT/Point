import time

from doublegis_api.api import Api2Gis
from export.excel_export import export_excel
from filtering.categories import get_filter_cats_ids, filter_by_categories

start_time = time.time()

print()
api = Api2Gis()
api.load()
api.describe()

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
