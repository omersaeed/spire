from bake import *
from scheme import *

class StartWsgiServer(Task):
    name = 'spire.wsgi'
    description = 'starts a spire server using the wsgi driver'
    parameters = {
        'address': Text(description='hostname:port', required=True),
        'config': Text(description='path to configuration file', required=True),
    }

    def run(self, runtime):
        from spire.drivers.wsgi import Driver
        Driver(self['address'], self['config'])
