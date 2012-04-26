from scheme import Sequence, Text, Tuple
from scheme.supplemental import ObjectReference
from werkzeug.exceptions import HTTPException, InternalServerError, NotFound
from werkzeug.local import Local, release_local
from werkzeug.routing import Map, RequestRedirect, Rule
from werkzeug.wrappers import Request as WsgiRequest, Response

from spire.assembly import Configuration, Dependency, configured_property
from spire.util import call_with_supported_params, enumerate_modules, is_class, is_module, is_package
from spire.wsgi.sessions import SessionManager
from spire.wsgi.templates import TemplateEnvironment
from spire.wsgi.util import Mount

LOCAL = Local()

class Request(WsgiRequest):
    """A WSGI request."""

    def __init__(self, application, environ, urls, context=None):
        super(Request, self).__init__(environ)
        self.application = application
        self.context = context or {}
        self.endpoint = None
        self.params = None
        self.session = environ.get('spire.session')
        self.template_context = {}
        self.urls = urls

    def bind(self):
        LOCAL.request = self

    @classmethod
    def current_request(cls):
        try:
            return LOCAL.request
        except AttributeError:
            return None

    def match(self):
        if self.endpoint:
            return

        try:
            self.endpoint, self.params = self.urls.match()
        except (HTTPException, RequestRedirect), error:
            return error
        else:
            self.params['endpoint'] = self.endpoint

    def render(self, template, context=None, response=None, mimetype='text/html', **params):
        response = response or Response(mimetype=mimetype)
        response.data = self.render_template(template, context, **params)
        return response

    def render_template(self, template, context=None, **params):
        template_context = self.template_context
        for contribution in (context, params):
            if contribution:
                template_context.update(contribution)

        return self.application.environment.render_template(template, template_context)

    def unbind(self):
        release_local(LOCAL)

    def url_for(self, endpoint, **params):
        return self.urls.build(endpoint, params)

class Application(Mount):
    """A WSGI application."""

    configuration = Configuration({
        'mediators': Sequence(ObjectReference(nonnull=True)),
        'middleware': Sequence(ObjectReference(nonnull=True)),
        'templates': Sequence(Tuple((Text(nonempty=True), Text(nonempty=True)))),
        'urls': ObjectReference(nonnull=True, required=True),
        'views': Sequence(ObjectReference(nonnull=True), unique=True),
    })

    sessions = Dependency(SessionManager)

    def __init__(self, urls, views=None, templates=None, mediators=None, middleware=None):
        if isinstance(urls, (list, tuple)):
            urls = Map(list(urls))
        if not isinstance(urls, Map):
            raise Exception()

        self.urls = urls
        self.views = self._collect_views(views)

        self.environment = None
        if templates:
            self.environment = TemplateEnvironment(templates)

        self.mediators = []
        if mediators:
            for mediator in mediators:
                self.mediators.append(mediator())

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
        request = Request(self, environ, urls)
        response = None

        request.bind()
        try:

            mediators = self.mediators
            for mediator in mediators:
                response = mediator.mediate_request(request)
                if response:
                    break

            view = None
            if not response:
                response = request.match()
                if not response:
                    view = self.views.get(request.endpoint)
                    if not view:
                        view = self.views.get('default')
                    if not view:
                        response = NotFound()

            
            if not response:
                try:
                    response = call_with_supported_params(view, request, **request.params)
                    if not isinstance(response, Response):
                        response = Response(response)
                except Exception, exception:
                    for mediator in reversed(mediators):
                        response = mediator.mediate_exception(request, exception)
                        if response:
                            break
                    else:
                        import traceback;traceback.print_exc()
                        response = InternalServerError()

            for mediator in reversed(mediators):
                response = mediator.mediate_response(request, response) or response

            return response

        finally:
            request.unbind()

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

class Mediator(object):
    """A request/response mediator."""

    def mediate_exception(self, request, exception):
        return None

    def mediate_request(self, request):
        return None

    def mediate_response(self, request, response):
        return response

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
