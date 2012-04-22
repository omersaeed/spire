from __future__ import absolute_import

from mesh.transport.http import HttpClient, HttpServer
from scheme import *
from scheme.supplemental import ObjectReference

from spire.assembly import Configuration, Dependency, configured_property
from spire.wsgi.util import Mount
from spire.unit import Unit

class ClientContext(object):
    def __init__(self, unit, context):
        self.context = context
        self.unit = unit

    def __enter__(self):
        self.client = self.unit.instantiate(self.context)
        self.client.register()
        return self.client

    def __exit__(self, *args):
        self.client.unregister()

class MeshClient(Unit):
    configuration = Configuration({
        'client': ObjectReference(nonnull=True, required=True, default=HttpClient),
        'specification': ObjectReference(nonnull=True, required=True),
        'url': Text(nonempty=True),
    })

    client = configured_property('client')
    specification = configured_property('specification')
    url = configured_property('url')

    def __call__(self, context=None):
        return ClientContext(self, context)

    def instantiate(self, context):
        return self.client(self.url, self.specification, context)

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
