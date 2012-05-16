import sys
from os import path

import yaml
from werkzeug.wsgi import SharedDataMiddleware

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

        config = self.get_config(configuration)
        if 'static-map' in config:
            m = config['static-map'].split('=')
            self.dispatcher = SharedDataMiddleware(self.dispatcher, {
                m[0]: path.abspath(m[1])
                }, cache=False);

        self.server = WsgiServer(address, self.dispatcher)
        self.server.serve()

    def get_config(self, filename):
        config = yaml.load(open(filename))
        return config['wsgi'] if config and 'wsgi' in config else {}


if __name__ == '__main__':
    Driver(*sys.argv[1:])

