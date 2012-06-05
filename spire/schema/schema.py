from threading import Lock

from mesh.standard import OperationError, ValidationError

from scheme import Boolean, Text
from scheme.supplemental import ObjectReference
from sqlalchemy import MetaData, create_engine, event
from sqlalchemy.orm.session import sessionmaker

from spire.core import *
from spire.local import ContextLocals
from spire.schema.pool import EnginePool

__all__ = ('OperationError', 'Schema', 'SchemaDependency', 'SchemaInterface', 'ValidationError')

SessionLocals = ContextLocals.create_prefixed_proxy('schema.session')

class Schema(object):
    """A spire schema."""

    guard = Lock()
    schemas = {}

    def __new__(cls, name):
        cls.guard.acquire()
        try:
            try:
                return cls.schemas[name]
            except KeyError:
                instance = cls.schemas[name] = super(Schema, cls).__new__(cls)
                instance.constructors = []
                instance.name = name
                instance.metadata = MetaData()
                SchemaDependency.register(name)
                SessionLocals.declare(name)
                return instance
        finally:
            cls.guard.release()

    @classmethod
    def constructor(cls, name):
        def decorator(function):
            cls(name).constructors.append(function)
            return function
        return decorator

    @classmethod
    def interface(cls, name):
        return SchemaDependency(name).get()

class SchemaInterface(Unit):
    configuration = Configuration({
        'echo': Boolean(default=False),
        'pool': ObjectReference(default=EnginePool),
        'schema': Text(nonempty=True),
        'url': Text(nonempty=True),
    })

    def __init__(self, schema, pool):
        if isinstance(schema, basestring):
            schema = Schema.schemas[schema]
    
        self.pool = pool(self.configuration)
        self.schema = schema

    @property
    def session(self):
        return self.get_session()

    def get_engine(self, **params):
        return self.pool.get_engine(**params)

    def get_session(self, independent=False, **params):
        if independent:
            return self.pool.get_session(**params)

        session = SessionLocals.get(self.schema.name)
        if session:
            return session

        session = self.pool.get_session(**params)
        return SessionLocals.push(self.schema.name, session, session.close)

    def create_tables(self, **params):
        self.schema.metadata.create_all(self.get_engine(**params))
        session = self.get_session(True, **params)

        for constructor in self.schema.constructors:
            constructor(session)
        return self

    def drop_tables(self, **params):
        self.schema.metadata.drop_all(self.get_engine(**params))
        return self

class SchemaDependency(Dependency):
    def __init__(self, schema, **params):
        self.schema = schema
        super(SchemaDependency, self).__init__(SchemaInterface, 'schema:%s' % schema, **params)

    def contribute_params(self):
        return {'schema': self.schema}
