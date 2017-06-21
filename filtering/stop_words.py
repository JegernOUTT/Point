import json
import numpy as np


def get_stop_words(filename='filtering/stop_words.json'):
    with open(filename, 'r') as f:
        data = json.load(f)
    return [d.lower().strip() for d in data]


def _filial_name_in_stop(name, stop_words):
    return np.any([word in name.lower() for word in stop_words])


def filter_by_stop_words(filials, organizations, stop_words, progress='ipython'):
    if progress == 'ipython':
        from utility.ipython_utility import log_progress

        filtered = []
        for filial in log_progress(filials, name='Филиалы', every=1):
            org = next(o for o in organizations if o.id == filial.organization_id)
            if not _filial_name_in_stop(org.name, stop_words):
                filtered.append(filial)
        return np.array(filtered)

    else:
        from tqdm import tqdm

        filtered = []
        for filial in tqdm(filials):
            if _filial_name_in_stop(filial.name, stop_words):
                filtered.append(filial)
        return np.array(filtered)
