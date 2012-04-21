from __future__ import absolute_import

from mesh.transport.http import HttpClient, HttpServer
from scheme import *
from scheme.supplemental import ObjectReference

from spire.wsgi.util import Mount
from spire.unit import Configuration, Dependency, Unit

class MeshClient(Unit):
    abstract = True
    configuration = Configuration({
        'client': ObjectReference(nonnull=True, default=HttpClient),
        'url': Text(nonempty=True),
    })

class MeshIntermediary(Mount):
    abstract = True
    configuration = Configuration({
        
    })

class MeshDependency(Dependency):
    """A mesh dependency."""

    def __init__(self, name, version, intermediary=False, optional=False, deferred=True, **params):
        self.name = name
        self.version = version

        unit = MeshClient
        if intermediary:
            deferred = False
            unit = MeshIntermediary

        token = 'mesh:%s-%s' % (name, version)
        super(MeshDependency, self).__init__(unit, token, optional, deferred, **params)

    def contribute(self):
        return {'name': self.name, 'version': self.version}

class MeshServer(Mount):
    abstract = True
    configuration = Configuration({
        'bundles': Union((
            Sequence(ObjectReference(nonnull=True), unique=True),
            Map(Text(nonempty=True)),
        ), required=True),
        'server': ObjectReference(nonnull=True, default=HttpServer),
    })

    def __init__(self, bundles, server):
        self.application = server(bundles)
