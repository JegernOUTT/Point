import numpy as np
from utility.distance import distance


def add_metro_distances(filial, metros):
    filial.metro_distances_json = {m.id: distance(m.latitude, m.longitude, filial.latitude, filial.longitude)
                                   for m in metros}
    return filial


def get_nearest_stations(filial, metros, count=1):
    assert count >= 1
    assert count <= len(metros)
    distances = np.array([(next(m for m in metros if m.id == k), v)
                          for k, v in filial.metro_distances_json.items()], dtype=[('metro', 'O'),
                                                                                   ('distance', int)])
    return np.sort(distances, order='distance')[:count]
