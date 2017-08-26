import uuid
import json
from operator import itemgetter

from jsonrpc.jsonrpc2 import JSONRPC20Request
from stevedore.driver import DriverManager
from stevedore.extension import ExtensionManager

DOR_REQUEST_NAMESPACE = 'dor_ops.requests'


class BaseRequest(JSONRPC20Request):

    _required_params = ()
    _optional_params = ('formatter',)

    def __init__(self, service, method, **kwargs):
        self.service = service
        self.service_method = method

        method = "%s.%s" % (service, method)
        params = kwargs.get('params', self._set_params(kwargs))

        is_notification = kwargs.get('is_notification', False)
        if is_notification:
            id = None
        else:
            id = kwargs.get('_id', str(uuid.uuid4()))
        super(BaseRequest, self).__init__(method,
                                          params,
                                          id,
                                          is_notification=is_notification)

    def __getattr__(self, name):
        """ Looks into self.params dict for the needed value.

        :param str name: name of attribute
        :return: attribute value
        """
        try:
            return self.params[name]
        except KeyError:
            return None

    @property
    def id(self):
        return self._id

    @classmethod
    def required_params(cls):
        return cls._required_params

    @classmethod
    def optional_params(cls):
        return cls._optional_params

    @classmethod
    def all_params(cls):
        return cls.required_params() + cls.optional_params()

    def _set_params(self, kwargs):
        params = {}
        for key, val in kwargs.items():
            if val and key in self.__class__.all_params():
                params[key] = self._loads_if_json(val)
        return params

    def _loads_if_json(self, val):
        try:
            return json.loads(val)
        except (ValueError, TypeError):
            return val

    @staticmethod
    def get_available_request_names():
        return ExtensionManager(DOR_REQUEST_NAMESPACE).names()

    @staticmethod
    def get_available_requests_by_service():
        names = BaseRequest.get_available_request_names()
        split_names = map(lambda x: x.split('.'), names)
        # get a collection of unique service names
        services = set(map(itemgetter(0), split_names))

        def method_map(service, name_list):
            return map(itemgetter(1),
                       filter(lambda x: x[0] == service, name_list))

        return {service: method_map(service, split_names)
                for service in services}


class RequestFactory(object):

    @staticmethod
    def create_request_from_json(body):
        json_request = JSONRPC20Request.from_json(body)
        request_mgr = DriverManager(namespace=DOR_REQUEST_NAMESPACE,
                                    name=json_request.method,
                                    invoke_kwds=json_request.params,
                                    invoke_on_load=True)
        driver = request_mgr.driver
        driver._id = json.loads(body)['id']  # preserve original id
        return driver

    @staticmethod
    def create_request(service, method, request_kwargs=None, invoke=True):
        service_name = "%s.%s" % (service, method)
        request_mgr = DriverManager(namespace=DOR_REQUEST_NAMESPACE,
                                    name=service_name,
                                    invoke_kwds=request_kwargs or {},
                                    invoke_on_load=invoke)
        return request_mgr.driver

    @staticmethod
    def request_params(service, method):
        method_class = RequestFactory.create_request(service, method,
                                                     invoke=False)
        return {'required': method_class.required_params(),
                'optional': method_class.optional_params()}
