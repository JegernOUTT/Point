import sys


def safe_request(session, verbose=False, allowed_codes={200}, timeout=3, **kwargs):
    while True:
        try:
            r = session.get(**kwargs, timeout=timeout)
            if r.status_code in allowed_codes:
                return r
            else:
                if verbose:
                    print('Error in request: {0}'.format('Status code == ' + r.status_code),
                          file=sys.stderr)
                continue
        except Exception as e:
            if verbose:
                print('Error in request: {0}'.format(e), file=sys.stderr)
            continue
