from __future__ import absolute_import

from mesh.transport.http import HttpClient, HttpServer
from scheme import *
from scheme.supplemental import ObjectReference

from spire.assembly import Configuration, Dependency, configured_property
from spire.wsgi.application import Request
from spire.wsgi.util import Mount
from spire.unit import Unit

class MeshClient(Unit):
    configuration = Configuration({
        'client': ObjectReference(nonnull=True, required=True, default=HttpClient),
        'specification': ObjectReference(nonnull=True, required=True),
        'url': Text(nonempty=True),
    })

    def __init__(self, client, specification, url):
        self.instance = client(url, specification, self._construct_context)
        self.instance.register()

    def _construct_context(self):
        request = Request.current_request()
        if request:
            return request.context

class MeshDependency(Dependency):
    def __init__(self, name, version, optional=False, deferred=False, **params):
        self.name = name
        self.version = version
        unit = MeshClient

        token = 'mesh:%s-%s' % (name, version)
        super(MeshDependency, self).__init__(unit, token, optional, deferred, **params)

class MeshServer(Mount):
    configuration = Configuration({
        'bundles': Sequence(ObjectReference(notnull=True), required=True, unique=True),
        'server': ObjectReference(nonnull=True, default=HttpServer),
    })

    def __init__(self, bundles, server):
        self.application = server(bundles)
