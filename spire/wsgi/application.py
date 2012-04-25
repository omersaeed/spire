from scheme import Sequence, Text, Tuple
from scheme.supplemental import ObjectReference
from werkzeug.exceptions import HTTPException, InternalServerError, NotFound
from werkzeug.routing import Map, Rule
from werkzeug.wrappers import Request as WsgiRequest, Response

from spire.unit import Configuration, Dependency
from spire.util import call_with_supported_params, enumerate_modules, is_class, is_module, is_package
from spire.wsgi.sessions import SessionManager
from spire.wsgi.templates import TemplateEnvironment
from spire.wsgi.util import Mount

class Request(WsgiRequest):
    """A WSGI request."""

    def __init__(self, environ, application, urls, params):
        super(Request, self).__init__(environ)
        self.application = application
        self.params = params
        self.session = environ.get('spire.session')
        self.urls = urls

    def render(self, template, context=None, response=None, mimetype='text/html', **params):
        response = response or Response(mimetype=mimetype)
        response.data = self.render_template(template, context, **params)
        return response

    def render_template(self, template, context=None, **params):
        template_context = self.environ.get('spire.template_context') or {}
        for contribution in (context, params):
            if contribution:
                template_context.update(contribution)

        return self.application.environment.render_template(template, template_context)

class Application(Mount):
    """A WSGI application."""

    configuration = Configuration({
        'middleware': Sequence(ObjectReference(nonnull=True)),
        'templates': Sequence(Tuple((Text(nonempty=True), Text(nonempty=True)))),
        'urls': ObjectReference(nonnull=True, required=True),
        'views': Sequence(ObjectReference(nonnull=True), unique=True),
    })

    sessions = Dependency(SessionManager)

    def __init__(self, urls, views=None, templates=None, middleware=None):
        if isinstance(urls, (list, tuple)):
            urls = Map(list(urls))
        if not isinstance(urls, Map):
            raise Exception()

        self.urls = urls
        self.views = self._collect_views(views)

        self.environment = None
        if templates:
            self.environment = TemplateEnvironment(templates)

        self.application = self.dispatch
        if middleware:
            for implementation in reversed(middleware):
                self.application = implementation(self.application)

    def __call__(self, environ, start_response):
        session = None
        if self.sessions.enabled:
            session = self.sessions.get(environ)

        environ.update({
            'spire.application': self,
            'spire.session': session,
            'spire.template_context': {},
        })

        try:
            response = self.application(environ, start_response)
        except HTTPException, error:
            response = error
        except Exception, error:
            raise
            response = InternalServerError()

        if session and session.should_save:
            self.sessions.save(session, response)

        return response(environ, start_response)

    def dispatch(self, environ, start_response):
        urls = self.urls.bind_to_environ(environ)
        endpoint, params = urls.match()
        params['endpoint'] = endpoint

        view = self.views.get(endpoint)
        if not view:
            view = self.views.get('default')
        if not view:
            return NotFound()

        request = Request(environ, self, urls, params)
        response = call_with_supported_params(view, request, **params)

        if not isinstance(response, Response):
            response = Response(response)
        return response

    def _collect_views(self, targets):
        views = {}
        for target in (targets or []):
            if is_package(target):
                modules = enumerate_modules(target, True)
            elif is_module(target):
                modules = [target]
            else:
                raise Example()

            for module in modules:
                for attr, value in module.__dict__.iteritems():
                    try:
                        viewable = getattr(value, '__viewable__', None)
                    except Exception:
                        viewable = False
                    if viewable:
                        if is_class(value):
                            value = value()
                        views[value.endpoint] = value
        return views

class Middleware(object):
    """WSGI middleware."""

    def __init__(self, application):
        self.application = application

    def __call__(self, environ, start_response):
        request = WsgiRequest(environ, shallow=True)
        self.process_request(request, environ)

        response = self.application(environ, start_response)
        self.process_response(request, response)
        return response

    def process_request(self, request, environ):
        pass

    def process_response(self, request, response):
        pass

def view(endpoint):
    if hasattr(endpoint, '__call__'):
        endpoint.__viewable__ = True
        endpoint.endpoint = endpoint.__name__
        return endpoint
    else:
        def decorator(obj):
            obj.__viewable__ = True
            obj.endpoint = endpoint
            return obj
        return decorator
