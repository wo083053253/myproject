import errno
import logging
import shlex

from abc import ABCMeta
from stevedore.driver import DriverManager
from subprocess import Popen, PIPE
from dor_ops.common.config import BaseConfig

DOR_SERVICE_NAMESPACE = 'dor_ops.services'

LOG = logging.getLogger(__name__)


class BaseService(object):

    __metaclass__ = ABCMeta

    def __init__(self, default_driver=None):
        self.name = self.__class__.__name__
        self._default_driver = default_driver

    @property
    def request_type(self):
        return self._request_type

    def handle_request(self, request):
        driver = request.driver or self._default_driver
        LOG.debug("Received a '%s' request with driver: %s",
                  request.service, driver)

        handler = self
        try:
            if driver:
                handler = DriverFactory.create_driver(request.service, driver)
        except Exception as e:
            LOG.error('Could not instantiate driver: %s', driver)
            raise(e)

        func = getattr(handler, request.service_method)
        return func(request)

    def call_external_service(self, command, background=True):
        """Invokes shell command

        Args:
            background: If True will fork command and not wait for it to finish

        Returns:
            (0, stdout) on success
            (-32101, None) on "No such file or directory" error
            (-32102, stderr|stdout|None) on all other errors
        """
        LOG.debug("Calling external service: %s", command)

        try:
            command = shlex.split(command)
            if background:
                # fork background process
                Popen(command)
                return (0, None)
            else:
                # wait for process to exit
                p = Popen(command, stdout=PIPE, stderr=PIPE)
                (stdout, stderr) = p.communicate()
                if p.returncode != 0:
                    LOG.debug("External service call failed: %s: %s"
                              % (p.returncode, stderr or stdout))
                    return (-32102, stderr or stdout)
                return (0, stdout)
        except OSError as e:
            LOG.debug("External service call failed: %s", e.strerror)
            if e.errno == errno.ENOENT:
                return (-32101, None)  # No such file or directory
            else:
                return (-32102, None)  # all other errors


class ServiceFactory(object):

    @staticmethod
    def create_service(service_name, config_path=None):
        service_mgr = DriverManager(namespace=DOR_SERVICE_NAMESPACE,
                                    name=service_name,
                                    invoke_on_load=False)
        service_cls = service_mgr.driver

        if config_path and issubclass(service_cls, BaseConfig):
            return service_cls.from_disk(config_path, required=False)
        else:
            return service_cls()


class DriverFactory(object):

    @staticmethod
    def create_driver(service_name, driver_name):
        namespace = ".".join([DOR_SERVICE_NAMESPACE,
                              service_name, 'drivers'])
        driver_mgr = DriverManager(namespace=namespace,
                                   name=driver_name,
                                   invoke_on_load=True)
        return driver_mgr.driver
