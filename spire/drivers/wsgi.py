import sys

from spire.drivers.driver import Driver
from spire.wsgi.server import WsgiServer
from spire.wsgi.util import Dispatcher, Mount

class Driver(Driver):
    def __init__(self, address, configuration=None, assembly=None):
        super(Driver, self).__init__(configuration, assembly)
        self.deploy()

        self.dispatcher = Dispatcher()
        for unit in self.assembly.collate(Mount):
            self.dispatcher.mount(unit.configuration['path'], unit)

        self.server = WsgiServer(address, self.dispatcher)
        self.server.serve()

if __name__ == '__main__':
    Driver(*sys.argv[1:])
