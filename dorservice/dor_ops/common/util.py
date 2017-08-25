import sys
import logging
import cStringIO

def setup_logger(debug=False):
    """ Configure a root logger. All messages propagated here """
    logger = logging.getLogger()
    level = logging.INFO
    if debug:
        level = logging.DEBUG
    logger.setLevel(level)

    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter(fmt='%(asctime)s %(threadName)s %(name)s '
                            '%(levelname)s: %(message)s',
                            datefmt='%F %H:%M:%S')
    handler.setFormatter(fmt)
    logger.addHandler(handler)

def log_voluptuous_validation_errors(logger_name, exc, data=None):
    """ Logs every error in `MultipleInvalid.errors` to a logger's error
    handler.  Takes a logger name so that we don't lose the source. Optionally
    takes the original validated data structure to assist in error messages and
    a logging facility.

    :param logger_name: module path, i.e. `__name__`.  Assumes logger is setup.
    :param exc: `MultipleInvalid`
    :param data: original data structure validated by voluptuous
    """
    logger = logging.getLogger(logger_name)
    for err in exc.errors:
        if data is not None:
            orig = get_nested(data, err.path)
            err_str = "%s; got '%s', '%s'" % (err, orig, type(orig))
        else:
            err_str = str(err)
        logger.error(err_str)
        
def get_nested(data, path):
    """ Given path, a list of keys/indexes, recurse down into `data` and return
    the value at the path

    Given:
    data: `{'bar': 'moo', 'baz': [1, 2, 3], 'foo': 1}`
    path: `['baz', 2]`

       get_nested(data, path)  # returns 3

    :param data: a list/dict
    :param path: list of strings and ints, successive proper arguments to an
    objects `__getitem__` method
    :return: value at `path` from `data`
    """
    try:
        curr_path = path[0]
    except IndexError:
        return

    data_from_path = data[curr_path]
    if len(path) == 1:
        return data_from_path
    return get_nested(data_from_path, path[1:])
