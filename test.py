# test.py
import dateutil.parser
import jsonpickle
import numpy as np

from nameko.web.handlers import http

from doublegis_api.api import Api2Gis


class HttpService:
    api = Api2Gis()
    name = "http_service"

    @http('GET', '/get_filials')
    def get_filials(self, request):
        count = int(request.values['count'])
        return jsonpickle.encode(list(map(lambda x: x, HttpService.api.filials[:count])))

    @http('GET', '/get_oldest_filials')
    def get_oldest_filials(self, request):
        count = int(request.values['count'])

        filials = np.sort(np.array(
            list(map(lambda x: (dateutil.parser.parse(x.created_at_json['2gis_appear_at']).replace(tzinfo=None).strftime("%s"), x),
                     HttpService.api.filials[HttpService.filials_with_date_indices])),
            dtype=[('time', np.uint64), ('filial', 'O')]), order='time')

        return jsonpickle.encode(list(map(lambda x: x[1], filials[:count])))

    @http('GET', '/get_newest_filials')
    def get_newest_filials(self, request):
        count = int(request.values['count'])

        filials = np.sort(np.array(
            list(map(lambda x: (dateutil.parser.parse(x.created_at_json['2gis_appear_at']).replace(tzinfo=None).strftime("%s"), x),
                     HttpService.api.filials[HttpService.filials_with_date_indices])),
            dtype=[('time', np.uint64), ('filial', 'O')]), order='time')[::-1]

        return jsonpickle.encode(list(map(lambda x: x[1], filials[:count])))

HttpService.api.load()
HttpService.filials_with_date_indices = [i
                                         for i, f in enumerate(HttpService.api.filials)
                                         if f.created_at_json['2gis_appear_at'] != '']
