from functools import reduce

import numpy as np
import json


def get_data_by_filial(api, filial):
    org = next(o for o in api.organizations if o.id == filial.organization_id)
    main_rubric = next(f for f in api.rubrics if f.id == int(org.main_rubrics['doublegis_rubrics_ids'][0]))
    sub_rubrics = [f for f in api.sub_rubrics
                   for i in org.sub_rubrics['doublegis_rubrics_ids'] if f.id == int(i)]
    return filial, org, main_rubric, sub_rubrics


def get_filter_cats_ids(rubrics, sub_rubrics, filename='filtering/categories.json'):
    with open(filename, 'r') as f:
        for_filter = json.load(f)
    main_categories_filter = [next(c.id for c in rubrics if c.name == rub)
                              for rub in [k for k, v in for_filter.items() if v == ['All']]]
    sub_categories_filter = []
    for category, values in for_filter.items():
        sub_categories_filter += [next(map(lambda x: x.id,
                                           filter(lambda x: x.name == sc, sub_rubrics)))
                                  for sc in values if sc in map(lambda x: x.name, sub_rubrics)]
    return set(main_categories_filter), \
           set(np.array(sub_categories_filter).reshape((len(sub_categories_filter),)))


def _filial_valid(filial, organizations, main, sub):
    org = next(o for o in organizations if o.id == filial.organization_id)
    in_main_filter = np.any([r in main for r in org.main_rubrics['doublegis_rubrics_ids']])
    in_sub_filter = np.all([r in sub for r in org.sub_rubrics['doublegis_rubrics_ids']])
    return (not in_main_filter) and (not in_sub_filter)


def filter_by_categories(filials, organizations,
                         main_categories_filter_ids, sub_categories_filter_ids, progress='ipython'):
    if progress == 'ipython':
        from utility.ipython_utility import log_progress

        filtered = []
        for filial in log_progress(filials, name='Филиалы'):
            if _filial_valid(filial, organizations, main_categories_filter_ids, sub_categories_filter_ids):
                filtered.append(filial)
        return np.array(filtered)

    else:
        from tqdm import tqdm

        filtered = []
        for filial in tqdm(filials):
            if _filial_valid(filial, organizations, main_categories_filter_ids, sub_categories_filter_ids):
                filtered.append(filial)
        return np.array(filtered)
