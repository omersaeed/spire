from scheme import Structure, Text
from scheme.supplemental import ObjectReference
from werkzeug.contrib.sessions import FilesystemSessionStore, SessionStore, Session

from spire.unit import Configuration, Unit

class SessionManager(Unit):
    """A session manager."""

    configuration = Configuration({
        'store': Structure(
            structure={
                FilesystemSessionStore: {
                    'path': Text(default=None),
                },
            },
            polymorphic_on=ObjectReference(name='implementation', nonnull=True),
            default={'implementation': FilesystemSessionStore},
        )
    })

    def __init__(self, store):
        print store
