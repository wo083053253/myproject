'''
Created on 2017年8月25日

@author: walter628
'''
import pika
import ssl

from voluptuous import Schema, Any, Optional, Invalid
from dor_ops.common.config import BaseConfig 

APP_ID = 'dor_ops'
DEFAULT_CONFIG_FILE = BaseConfig.get_configfile()
DEFAULT_WORKER_SVCS = []
DEFAULT_EXCHANGE = ''
DOR_EXCHANGE = 'dor_ops'
MESSENGER_EXCHANGE = 'messenger'
DIRECT_EXCHANGE_TYPE = 'direct'
DEFAULT_CONSUME_QUEUE = 'dor_ops'


def ssl_constant(value):
    if int(value) not in [ssl.CERT_NONE, ssl.CERT_OPTIONAL, ssl.CERT_REQUIRED]:
        raise Invalid("Invalid ssl constant")
    return value



class AgentConfig(BaseConfig):
    '''
    classdocs
    '''


    def __init__(self, params):
        '''
        Constructor
        '''
        