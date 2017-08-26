import argparse
import logging
import sys
import threading
import time
import os

from dor_ops.agent.agentconfig import AgentConfig
from dor_ops.services import ServiceFactory
from dor_ops.common.config import BaseConfig

LOG = logging.getLogger(__name__)

DEFAULT_CONFIG_FILE = BaseConfig.get_configfile()

def _consumer_thread(service_name, service, config):
    LOG.info("Starting %s consumer" % service_name)
    consumer = BpworkerConsumer(config.connection_params(),
                                routing_key=service_name,
                                exchange_type='direct',
                                queue=service_name,
                                handling_services=[service])
    consumer.run()

def startup(config_path):
    config = AgentConfig.from_disk(config_path)
    if not len(config.enabled_services):
        raise Exception("No services enabled.")

    threads = []
    for service_name in config.enabled_services:
        service = ServiceFactory.create_service(service_name, config_path)

        t = threading.Thread(name=service.name,
                             target=_consumer_thread,
                             args=(service_name, service, config))
        t.setDaemon(True)
        threads.append(t)
        t.start()

    while threading.active_count() > 0:
        time.sleep(1)

def parse_args():
    parser = argparse.ArgumentParser(description='The bpworker worker agent')

    parser.add_argument('-c', '--config', default=DEFAULT_CONFIG_FILE,
                        help='Configuration file to use')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Enable debug logging')
    parser.add_argument('-v', '--version', help='Get the bpworker version.')

    return parser.parse_args()
