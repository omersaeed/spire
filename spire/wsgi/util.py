from scheme import Text
from werkzeug.exceptions import NotFound

from spire.unit import Configuration, Unit

class Mount(Unit):
    abstract = True
    configuration = Configuration({
        'path': Text(description='url path', nonempty=True)
    })

    def __call__(self, environ, start_response):
        return self.application(environ, start_response)

class Dispatcher(object):
    def __init__(self, mounts=None):
        self.mounts = {}
        if mounts:
            for mount in mounts:
                self.mount(*mount)

    def dispatch(self, environ, start_response):
        script = environ.get('PATH_INFO', '')
        pathinfo = ''

        mounts = self.mounts
        while '/' in script:
            if script in mounts:
                application = mounts[script]
                break
            else:
                segments = script.split('/')
                script = '/'.join(segments[:-1])
                pathinfo = '/%s%s' % (segments[-1], pathinfo)
        else:
            application = mounts.get(script)
            if not application:
                return NotFound()(environ, start_response)

        environ['SCRIPT_NAME'] = environ.get('SCRIPT_NAME', '') + script
        environ['PATH_INFO'] = pathinfo
        return application(environ, start_response)

    __call__ = dispatch

    def mount(self, path, application):
        path = '/' + path.strip('/')
        if path not in self.mounts:
            self.mounts[path] = application


class Simple(Mount):
    abstract = True
    def __call__(self, environ, start_response):
        from pprint import pformat
        from werkzeug.wrappers import Response
        return Response(pformat(environ))(environ, start_response)
