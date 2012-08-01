import os
import sys

from werkzeug.wsgi import SharedDataMiddleware

from spire.runtime.runtime import Runtime
from spire.wsgi.server import WsgiServer
from spire.wsgi.util import Dispatcher, Mount

class Runtime(Runtime):
    def __init__(self, address, configuration=None, assembly=None):
        super(Runtime, self).__init__(configuration, assembly)
        self.deploy()
        self.startup()

        self.dispatcher = Dispatcher()
        for unit in self.assembly.collate(Mount):
            self.dispatcher.mount(unit.configuration['path'], unit)

        wsgi = self.configuration.get('wsgi')
        if wsgi and 'static-map' in wsgi:
            map = wsgi['static-map'].split('=')
            self.dispatcher = SharedDataMiddleware(self.dispatcher, {
                map[0]: os.path.abspath(map[1])
            }, cache=False)

        self.server = WsgiServer(address, self.dispatcher)
        self.server.serve()

if __name__ == '__main__':
    Runtime(*sys.argv[1:])
