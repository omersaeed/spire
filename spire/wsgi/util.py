from scheme import Sequence, Text
from werkzeug.exceptions import InternalServerError, NotFound

from spire.local import ContextLocals
from spire.core import Configuration, Unit

class Mount(Unit):
    configuration = Configuration({
        'middleware': Sequence(Text(nonempty=True), unique=True),
        'path': Text(description='url path', nonempty=True),
    })

    def __init__(self):
        try:
            self.application
        except AttributeError:
            self.application = self.dispatch

        middleware = self.configuration.get('middleware')
        if middleware:
            for attr in reversed(middleware):
                self.application = getattr(self, attr).wrap(self.application)

    def __call__(self, environ, start_response):
        try:
            return self.application(environ, start_response)
        except Exception:
            import traceback;traceback.print_exc()
            return InternalServerError()(environ, start_response)
        finally:
            ContextLocals.purge()

    def dispatch(self, environ, start_response):
        raise NotImplementedError()

class MiddlewareWrapper(object):
    def __init__(self, wrapper, application):
        self.application = application
        self.wrapper = wrapper

    def __call__(self, environ, start_response):
        return self.wrapper.dispatch(self.application, environ, start_response)

class Middleware(object):
    def wrap(self, application):
        return MiddlewareWrapper(self, application)

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
            application = mounts.get(script or '/')
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
