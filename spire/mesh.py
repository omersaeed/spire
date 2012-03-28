from scheme import *
from spire.unit import Dependency, Unit

class MeshClient(Unit):
    configuration = Structure({
        'client': Text('client', required=True, nonnull=True),
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

