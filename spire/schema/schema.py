from threading import Lock

from scheme import Boolean, Text
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm.session import sessionmaker

from spire.core import *
from spire.local import ContextLocals

__all__ = ('Schema', 'SchemaDependency', 'SchemaInterface')

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
        'schema': Text(nonempty=True),
        'url': Text(nonempty=True),
    })

    def __init__(self, schema, url, echo=False):
        if isinstance(schema, basestring):
            schema = Schema.schemas[schema]

        self.engine = create_engine(url, echo=echo)
        self.schema = schema
        self.session_maker = sessionmaker(bind=self.engine)

    @property
    def session(self):
        return self.get_session()

    def get_session(self, independent=False):
        if independent:
            return self.session_maker()

        session = SessionLocals.get(self.schema.name)
        if session:
            return session

        session = self.session_maker()
        return SessionLocals.push(self.schema.name, session, session.close)

    def create_tables(self):
        self.schema.metadata.create_all(self.engine)
        for constructor in self.schema.constructors:
            constructor(self)
        return self

    def drop_tables(self):
        self.schema.metadata.drop_all(self.engine)
        return self

class SchemaDependency(Dependency):
    def __init__(self, schema, **params):
        self.schema = schema
        super(SchemaDependency, self).__init__(SchemaInterface, 'schema:%s' % schema, **params)

    def contribute_params(self):
        return {'schema': self.schema}
