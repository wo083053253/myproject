import logging

from novaclient import client as novac
from keystoneclient import client as keystonec
from neutronclient import client as neutronc
from glanceclient import client as glancec

from dor_ops.services.opsapi.drivers import BaseDriver

LOG = logging.getLogger(__name__)

class OPSDriver(BaseDriver):
    
    def __init__(self):
        print "hello word!"
    
    def setup_keypair(self):
        return
    
    def setup_secgroup(self):
        return
    
    def setup_subnet(self):
        return
    
    def boot_vm(self):
        return