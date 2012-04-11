from scheme.formats import Format

from spire.container import Container
from spire.wsgi import Dispatcher

try:
    import uwsgi
except ImportError:
    uwsgi = None

class Driver(object):
    def __init__(self):
        components, configuration = self._load_configuration()
        self.container = Container(components)
        self.container.assemble(configuration)
        self.dispatcher = Dispatcher(self.container)

    def __call__(self, environ, start_response):
        return self.dispatcher.dispatch(environ, start_response)

    def _load_configuration(self):
        for key in ('yaml', 'yml', 'json'):
            filename = uwsgi.opt.get(key)
            if filename:
                break
        else:
            raise RuntimeError()

        configuration = Format.read(filename)
        if 'spire' in configuration:
            configuration = configuration['spire']
            if 'components' in configuration and 'configuration' in configuration:
                return configuration['components'], configuration['configuration']
            else:
                raise RuntimeError()
        else:
            raise RuntimeError()

if uwsgi:
    uwsgi.applications = {'': Driver()}
