from mesh.transport.http import HttpClient, WsgiServer
from scheme import *
from scheme.supplemental import ObjectReference
from spire.component import Service
from spire.unit import Dependency, Unit

class MeshClient(Unit):
    configuration = Structure({
        'client': ObjectReference('client', required=True, nonnull=True),
        'target': Text('target', required=True, nonnull=True),
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
        'server': ObjectReference('server', nonnull=True, default=WsgiServer),
        'bundles': Sequence(ObjectReference(nonnull=True), required=True),
    }, required=True, nonnull=True)

    def __init__(self, configuration):
        pass
