from scheme import Boolean, Integer, Structure, Text
from scheme.supplemental import ObjectReference
from werkzeug.contrib.sessions import FilesystemSessionStore, SessionStore, Session
from werkzeug.utils import dump_cookie, parse_cookie

from spire import Configuration, Unit, configured_property
from spire.util import pruned

class SessionManager(Unit):
    """A session manager."""

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

    def get(self, environ):
        cookie = parse_cookie(environ.get('HTTP_COOKIE', ''))
        id = cookie.get(self.configuration['cookie']['name'])
        if id is not None:
            return self.store.get(id)
        else:
            return self.store.new()

    def save(self, session, response):
        self.store.save(session)
        self._set_cookie(response, session.sid)

    def _set_cookie(self, response, value):
        params = self.configuration['cookie']
        response.set_cookie(
            key=params['name'],
            value=value,
            max_age=params.get('max_age'),
            expires=params.get('expires'),
            domain=params.get('domain'),
            path=params.get('path'))
