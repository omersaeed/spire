from scheme import Text
from werkzeug.exceptions import NotFound
from werkzeug.wrappers import Request, Response

from spire.unit import Unit

class Application(Unit):
    __abstract__ = True
    configuration = {
        'path': Text(description='url path', required=True, nonnull=True),
    }

    def __call__(self, environ, start_response):
        return self.application(environ, start_response)

class Dispatcher(object):
    def __init__(self, assembly):
        self.mounts = {}
        for application in assembly.collate(Application):
            path = '/' + application.configuration['path'].strip('/')
            if path not in self.mounts:
                self.mounts[path] = application

    def dispatch(self, environ, start_response):
        script = environ.get('PATH_INFO', '')
        path_info = ''

        mounts = self.mounts
        while '/' in script:
            if script in mounts:
                application = mounts[script]
                break
            else:
                items = script.split('/')
                script = '/'.join(items[:-1])
                path_info = '/%s%s' % (items[-1], path_info)
        else:
            application = mounts.get(script)
            if not application:
                return NotFound()(environ, start_response)

        environ['SCRIPT_NAME'] = environ.get('SCRIPT_NAME', '') + script
        environ['PATH_INFO'] = path_info
        return application(environ, start_response)

def simple(environ, start_response):
    from pprint import pformat
    return Response(pformat(environ))(environ, start_response)

class Simple(Application):
    def assemble(self):
        self.application = simple
