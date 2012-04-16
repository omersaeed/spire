from mesh.transport.http import HttpClient, HttpServer
from scheme import *
from scheme.supplemental import ObjectReference

from spire.unit import Configuration, Dependency, Unit
from spire.wsgi import Application

class MeshClient(Unit):
    abstract = True
    configuration = Configuration({
        'client': ObjectReference(nonnull=True, default=HttpClient),
        'url': Text(nonempty=True),
    })

class MeshDependency(Dependency):
    """A mesh dependency."""

    def __init__(self, name, version, optional=False):
        self.name = name
        self.version = version

        token = 'mesh:%s/%s' % (name, version)
        super(MeshDependency, self).__init__(token, MeshClient, optional)

class MeshServer(Application):
    abstract = True
    configuration = Configuration({
        'bundles': Sequence(ObjectReference(nonnull=True), required=True, unique=True),
        'server': ObjectReference(nonnull=True, default=HttpServer),
    })

    def __init__(self, bundles):
        self.application = 
