from mesh.transport.http import HttpClient, HttpServer
from scheme import *
from scheme.supplemental import ObjectReference
from spire.component import Service
from spire.unit import Dependency, Unit

class MeshClient(Unit):
    configuration = Structure({
        'api': Text(required=True, nonnull=True),
        'client': ObjectReference(nonnull=True, default=HttpClient),
    }, required=True, nonnull=True)

    def __init__(self, configuration):
        pass

class MeshDependency(Dependency):
    """A mesh dependency."""

    def __init__(self, name, version, optional=False, scope=None):
        self.name = name
        self.version = version

        token = 'mesh:%s/%s' % (name, version)
        super(MeshDependency, self).__init__(token, MeshClient, optional, scope)

class MeshService(Service):
    configuration = Structure({
        'bundles': Sequence(ObjectReference(nonnull=True), required=True),
        'server': ObjectReference(nonnull=True, default=HttpServer),
    })

    def __init__(self, configuration):
        pass
