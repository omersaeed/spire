import sys

from scheme import Format

from spire.component import Registry
from spire.wsgi.server import WsgiServer
from spire.wsgi.util import Dispatcher, Mount

class Driver(object):
    def __init__(self, address, configuration):
        if isinstance(configuration, basestring):
            configuration = Format.read(configuration)
        if 'spire' in configuration:
            configuration = configuration['spire']
        else:
            raise RuntimeError()

        Registry.configure(configuration)
        Registry.deploy()

        self.dispatcher = Dispatcher()
        for unit in Registry.collate(Mount):
            self.dispatcher.mount(unit.configuration['path'], unit)

        self.server = WsgiServer(address, self.dispatcher)
        self.server.serve()

if __name__ == '__main__':
    Driver(*sys.argv[1:])
