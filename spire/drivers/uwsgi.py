from __future__ import absolute_import

from spire.drivers.driver import Driver
from spire.wsgi.util import Dispatcher, Mount

try:
    import uwsgi
except ImportError:
    uwsgi = None

class Driver(Driver):
    def __init__(self, assembly=None):
        super(Driver, self).__init__(assembly=assembly)
        for key in ('yaml', 'yml', 'json'):
            filename = uwsgi.opt.get(key)
            if filename:
                self.configure(filename)
                break
        else:
            raise RuntimeError()

        self.deploy()

        self.dispatcher = Dispatcher()
        for unit in self.assembly.collate(Mount):
            self.dispatcher.mount(unit.configuration['path'], unit)

    def __call__(self, environ, start_response):
        return self.dispatcher.dispatch(environ, start_response)

if uwsgi:
    uwsgi.applications = {'': Driver()}
