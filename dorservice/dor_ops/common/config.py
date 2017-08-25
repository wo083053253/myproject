import os
import logging
import StringIO
import ConfigParser

from dorservice.common.util import log_voluptuous_validation_errors

from voluptuous import MultipleInvalid

LOG = logging.getLogger(__name__)

class BaseConfig(object):
    '''
    classdocs
    '''
    @classmethod
    def from_string(cls, data, required=True):
        config = ConfigParser.ConfigParser()
        buf = StringIO.StringIO(data)
        config.readfp(buf)
        if not config.has_section(cls._config_section):
            if required:
                raise ConfigInvalidError("required section %s not found" %
                                         cls._config_section)
            return cls()

        data = dict(config.items(cls._config_section))
        try:
            validated = cls._config_schema(data)
        except AttributeError:
            validated = data
        except MultipleInvalid as e:
            log_voluptuous_validation_errors(__name__, e, data)
            raise
        return cls(**validated)

    @classmethod
    def from_file(cls, filename, required=True):
        if not os.path.isfile(filename):
            raise ConfigPathNotFoundError(filename)
        data = None
        with open(filename, 'rb') as fh:
            data = fh.read()
        return cls.from_string(data, required)

    @classmethod
    def from_confdir(cls, dirname, required=True, extension='ini'):
        files = []
        for _, _, filenames in os.walk(dirname):
            files.extend(filenames)
        files.sort()

        data = ""
        for filename in files:
            if not filename.endswith("." + extension):
                continue
            with open(os.path.join(dirname, filename), 'rb') as fh:
                data += fh.read()
        return cls.from_string(data, required)

    @classmethod
    def from_disk(cls, path, required=True):
        if os.path.isfile(path):
            return cls.from_file(path, required)
        elif os.path.isdir(path):
            return cls.from_confdir(path, required)
        raise ConfigPathNotFoundError(path)
    
class ConfigError(Exception):
    pass


class ConfigInvalidError(ConfigError):

    def __init__(self, msg=None):
        _msg = "Invalid configuration"
        if msg:
            _msg += ": %s" % msg
        super(ConfigInvalidError, self).__init__(_msg)


class ConfigPathNotFoundError(ConfigError):

    def __init__(self, path):
        msg = "Configuration not found at path: %s" % path
        super(ConfigPathNotFoundError, self).__init__(msg)
        