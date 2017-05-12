import multiprocessing
import requests
from multiprocessing.pool import Pool
import numpy as np


def request(session, **kwargs):
    while True:
        try:
            r = session.get(**kwargs, timeout=3)
            if r.status_code in {200, 404, 410}:
                return r
            else:
                continue
        except Exception as e:
            continue


def get_some(ids_range, same):
    with open('removed/ids_{0}.txt'.format(multiprocessing.current_process().pid), 'w') as f:
        pass

    session = requests.Session()
    for fil in ids_range:
        if fil in same:
            continue
        r = request(session=session,
                    url='https://2gis.ru/spb/firm/{0}'.format(fil))
        if r.status_code == 410:
            with open('removed/ids_{0}.txt'.format(multiprocessing.current_process().pid), 'a') as f:
                f.write('{0}\n'.format(fil))


def get_some_parallel(r, same):
    chunk_size = len(r) // 100
    pool = Pool(processes=100)
    results = [pool.apply_async(get_some, (r[chunk_size * i:chunk_size * (i + 1)], same))
               for i in range(100)]
    for async in results:
        async.get()


with open('removed_organizations', 'rb') as f:
    ids = np.load(f)
ids.shape[0]

r = list(range(70000001010000000, 70000001029999999))
r += list(range(5348552830000000, 5348552849999999))
print('All id count: {0}'.format(len(r)))
get_some_parallel(r, ids)