from scheme import Boolean, Integer, Structure, Text
from scheme.supplemental import ObjectReference
from werkzeug.contrib.sessions import FilesystemSessionStore, SessionStore, Session
from werkzeug.utils import dump_cookie, parse_cookie

from spire import Configuration, Unit, configured_property
from spire.wsgi import Mediator
from spire.util import pruned

class SessionMediator(Unit, Mediator):
    """A session mediator."""

    configuration = Configuration({
        'enabled': Boolean(default=True, required=True),
        'cookie': Structure({
            'name': Text(nonnull=True, min_length=1, default='sessionid'),
            'max_age': Integer(minimum=0),
        }, generate_default=True, required=True),
        'store': Structure(
            structure={
                FilesystemSessionStore: {
                    'path': Text(default=None),
                },
            },
            polymorphic_on=ObjectReference(name='implementation', nonnull=True),
            default={'implementation': FilesystemSessionStore},
            required=True,
        )
    })

    enabled = configured_property('enabled')

    def __init__(self, store):
        parameters = pruned(store, 'implementation')
        self.store = store['implementation'](**parameters)

    def get_session(self, environ):
        cookie = parse_cookie(environ.get('HTTP_COOKIE', ''))
        id = cookie.get(self.configuration['cookie']['name'])
        if id is not None:
            return self.store.get(id)
        else:
            return self.store.new()

    def mediate_request(self, request):
        request.session = None
        if self.enabled:
            request.session = self.get_session(request.environ)

    def mediate_response(self, request, response):
        session = request.session
        if session and session.should_save:
            self.save_session(session, response)

    def save_session(self, session, response):
        self.store.save(session)
        params = self.configuration['cookie']
        response.set_cookie(
            key=params['name'],
            value=session.sid,
            max_age=params.get('max_age'),
            expires=params.get('expires'),
            domain=params.get('domain'),
            path=params.get('path'))
