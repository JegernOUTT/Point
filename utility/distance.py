import math


def distance(lat1, long1, lat2, long2):
    rad = 6372795

    lat1 = lat1 * math.pi/180.
    lat2 = lat2 * math.pi/180.
    long1 = long1 * math.pi/180.
    long2 = long2 * math.pi/180.

    cl1 = math.cos(lat1)
    cl2 = math.cos(lat2)
    sl1 = math.sin(lat1)
    sl2 = math.sin(lat2)
    delta = long2 - long1
    cdelta = math.cos(delta)
    sdelta = math.sin(delta)

    y = math.sqrt(math.pow(cl2 * sdelta, 2) + math.pow(cl1 * sl2 - sl1 * cl2 * cdelta, 2))
    x = sl1 * sl2 + cl1 * cl2 * cdelta
    ad = math.atan2(y, x)

    return ad * rad