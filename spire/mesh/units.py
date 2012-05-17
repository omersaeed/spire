from __future__ import absolute_import

from mesh.transport.http import HttpClient, HttpProxy, HttpServer
from scheme import *
from scheme.supplemental import ObjectReference

from spire.core import *
from spire.context import ContextMiddleware, HeaderParser
from spire.local import ContextLocals
from spire.wsgi.application import Request
from spire.wsgi.util import Mount

__all__ = ('MeshClient', 'MeshProxy', 'MeshDependency', 'MeshServer')

CONTEXT_HEADER_PREFIX = 'X-SPIRE-'
ContextLocal = ContextLocals.declare('mesh.context')

class MeshClient(Unit):
    configuration = Configuration({
        'client': ObjectReference(nonnull=True, required=True, default=HttpClient),
        'specification': ObjectReference(nonnull=True, required=True),
        'url': Text(nonempty=True),
    })

    def __init__(self, client, specification, url):
        self.instance = client(url, specification, self._construct_context,
            context_header_prefix=CONTEXT_HEADER_PREFIX).register()

    def execute(self, *args, **params):
        return self.instance.execute(*args, **params)

    def _construct_context(self):
        context = ContextLocal.get()
        if context:
            return context

        request = Request.current_request()
        if request:
            return request.context

class MeshProxy(Mount):
    configuration = Configuration({
        'url': Text(nonempty=True),
    })

    def __init__(self, url):
        self.application = HttpProxy(url, self._construct_context, context_key='request.context',
            context_header_prefix=CONTEXT_HEADER_PREFIX)
        super(MeshProxy, self).__init__()

    def _construct_context(self):
        request = Request.current_request()
        if request:
            return request.context

class MeshDependency(Dependency):
    def __init__(self, name, version, proxy=False, optional=False, deferred=False,
            unit=None, **params):

        self.name = name
        self.version = version

        if proxy:
            token = 'mesh-proxy:%s-%s' % (name, version)
            unit = unit or MeshProxy
        else:
            token = 'mesh:%s-%s' % (name, version)
            unit = unit or MeshClient

        super(MeshDependency, self).__init__(unit, token, optional, deferred, **params)

class MeshServer(Mount):
    configuration = Configuration({
        'bundles': Sequence(ObjectReference(notnull=True), required=True, unique=True),
        'mediators': Sequence(Text(nonempty=True), nonnull=True, unique=True),
        'server': ObjectReference(nonnull=True, default=HttpServer),
    })

    def __init__(self, bundles, server, mediators=None):
        self.mediators = []
        if mediators:
            for mediator in mediators:
                self.mediators.append(getattr(self, mediator))

        super(MeshServer, self).__init__()
        self.server = server(bundles, mediators=self.mediators, context_key='request.context')

    def dispatch(self, environ, start_response):
        ContextLocal.push(environ.get('request.context'))
        try:
            return self.server(environ, start_response)
        finally:
            ContextLocal.pop()
