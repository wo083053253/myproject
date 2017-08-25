from abc import abstractmethod
import logging

class BaseDriver(object):
    
    @abstractmethod
    def setup_keypair(self):
        return
    
    @abstractmethod
    def setup_secgroup(self):
        return
    
    @abstractmethod
    def setup_network(self):
        return
    
    @abstractmethod
    def setup_subnet(self):
        return
    
    def boot_vm(self):
        return
      
    