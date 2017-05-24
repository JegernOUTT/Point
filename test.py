# test.py
from functools import reduce

import dateutil.parser
import jsonpickle
from colour import Color
import numpy as np

from nameko.web.handlers import http, HttpRequestHandler

from doublegis_api.api import Api2Gis


class CorsHttpRequestHandler(HttpRequestHandler):
    def handle_request(self, request):
        self.request = request
        return super(CorsHttpRequestHandler, self).handle_request(request)

    def response_from_result(self, *args, **kwargs):
        response = super(CorsHttpRequestHandler, self).response_from_result(*args, **kwargs)
        response.headers.add("Access-Control-Allow-Headers", self.request.headers.get("Access-Control-Request-Headers"))
        response.headers.add("Access-Control-Allow-Credentials", "true")
        response.headers.add("Access-Control-Allow-Methods", "*")
        response.headers.add("Access-Control-Allow-Origin", "*")
        return response

cors_http = CorsHttpRequestHandler.decorator


class HttpService:
    api = Api2Gis()
    name = "http_service"

    @cors_http('GET', '/get_organizations')
    def get_organizations(self, request):
        if 'count' in request.values.keys():
            count = int(request.values['count'])
        else:
            count = len(HttpService.api.organizations)
        return jsonpickle.encode(list(map(lambda x: x, HttpService.api.organizations[:count])))

    @cors_http('GET', '/get_filials')
    def get_filials(self, request):
        count = int(request.values['count'])
        return jsonpickle.encode(list(map(lambda x: x, HttpService.api.filials[:count])))

    @cors_http('GET', '/get_oldest_filials')
    def get_oldest_filials(self, request):
        count = int(request.values['count'])

        filials = np.sort(np.array(
            list(map(lambda x: (dateutil.parser.parse(x.created_at_json['2gis_appear_at']).replace(tzinfo=None).strftime("%s"), x),
                     HttpService.api.filials[HttpService.filials_with_date_indices])),
            dtype=[('time', np.uint64), ('filial', 'O')]), order='time')

        return jsonpickle.encode(list(map(lambda x: x[1], filials[:count])))

    @cors_http('GET', '/get_newest_filials')
    def get_newest_filials(self, request):
        count = int(request.values['count'])

        filials = np.sort(np.array(
            list(map(lambda x: (dateutil.parser.parse(x.created_at_json['2gis_appear_at']).replace(tzinfo=None).strftime("%s"), x),
                     HttpService.api.filials[HttpService.filials_with_date_indices])),
            dtype=[('time', np.uint64), ('filial', 'O')]), order='time')[::-1]

        return jsonpickle.encode(list(map(lambda x: x[1], filials[:count])))

    @cors_http('GET', '/get_removed_filials')
    def get_newest_filials(self, request):
        # count = int(request.values['count'])
        filials = list(filter(lambda x: x.closed_at_json['2gis_removed_at'] != '', HttpService.api.filials))
        return jsonpickle.encode(filials)

    @cors_http('GET', '/get_filials_by_org')
    def get_filials_by_org(self, request):
        org_id = int(request.values['org_id'])
        filials = list(filter(lambda x: org_id == x.organization_id,
                              HttpService.api.filials[HttpService.filials_with_date_indices]))

        def add_data(f):
            f.created_at_json['appear_normal'] = \
                dateutil.parser.parse(f.created_at_json['2gis_appear_at']).replace(tzinfo=None)
            return f
        filials = list(map(add_data, filials))
        if len(filials) == 0:
            return jsonpickle.encode([])

        dates = np.sort(np.unique(list(map(lambda x: x.created_at_json['appear_normal'], filials))))
        green, red = Color('green'), Color('red')
        color_range = list(green.range_to(red, len(dates)))
        color_date_dict = {dates[i]: color_range[i].hex_l for i in range(len(dates))}

        def add_colour(f):
            f.colour = color_date_dict[f.created_at_json['appear_normal']]
            return f

        final = list(map(add_colour, filials))

        return jsonpickle.encode(final)

HttpService.api.load()
HttpService.filials_with_date_indices = [i
                                         for i, f in enumerate(HttpService.api.filials)
                                         if f.created_at_json['2gis_appear_at'] != '']
