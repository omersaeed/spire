from threading import Lock

from mesh.standard import OperationError, ValidationError

from scheme import Boolean, Text
from scheme.supplemental import ObjectReference
from sqlalchemy import MetaData, create_engine, event
from sqlalchemy.orm.session import sessionmaker

from spire.core import *
from spire.local import ContextLocals
from spire.schema.dialect import get_dialect
from spire.schema.migration import MigrationInterface
from spire.util import get_package_path

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

    def constructor(self):
        def decorator(function):
            self.constructors.append(function)
            return function
        return decorator

    @classmethod
    def interface(cls, name):
        return SchemaDependency(name).get()

class SchemaInterface(Unit):
    configuration = Configuration({
        'admin_url': Text(nonnull=True),
        'echo': Boolean(default=False),
        'migrations': Text(nonnull=True),
        'schema': Text(nonempty=True),
        'url': Text(nonempty=True),
    })

    def __init__(self, schema, url):
        if isinstance(schema, basestring):
            schema = Schema.schemas[schema]

        self.cache = {}
        self.dialect = get_dialect(url)
        self.guard = Lock()
        self.schema = schema
        self.url = url

    @property
    def session(self):
        return self.get_session()

    def create_schema(self, **tokens):
        url = self._construct_url(tokens)
        name = url.split('/')[-1]

        admin_url = self.configuration.get('admin_url')
        self.dialect.create_database(admin_url, name)

        engine, sessions = self._acquire_engine(tokens)
        self.schema.metadata.create_all(engine)

        session = sessions()
        for constructor in self.schema.constructors:
            constructor(session)

        session.close()

        migrations = self._get_migration_interface()
        if migrations:
            migrations.stamp()

    def deploy_schema(self, **tokens):
        url = self._construct_url(tokens)
        name = url.split('/')[-1]

        admin_url = self.configuration.get('admin_url')
        if not self.dialect.is_database_present(admin_url, name):
            return self.create_schema(**tokens)

        migrations = self._get_migration_interface()
        if migrations:
            migrations.upgrade()

    def drop_schema(self, **tokens):
        url = self._construct_url(tokens)
        name = url.split('/')[-1]

        admin_url = self.configuration.get('admin_url')
        if admin_url:
            self.dialect.drop_database(admin_url, name)

    def get_engine(self, **tokens):
        engine, sessions = self._acquire_engine(tokens)
        return engine

    def get_session(self, independent=False, **tokens):
        if independent:
            engine, sessions = self._acquire_engine(tokens)
            return sessions()

        session = SessionLocals.get(self.schema.name)
        if session:
            return session

        engine, sessions = self._acquire_engine(tokens)
        session = sessions()
        return SessionLocals.push(self.schema.name, session, session.close)

    def _acquire_engine(self, tokens=None):
        url = self._construct_url(tokens)
        try:
            return self.cache[url]
        except KeyError:
            pass

        self.guard.acquire()
        try:
            if url in self.cache:
                return self.cache[url]

            engine, sessions = self._create_engine(url)
            self.cache[url] = (engine, sessions)
            return engine, sessions
        finally:
            self.guard.release()

    def _construct_url(self, tokens=None):
        url = self.url
        if tokens:
            url = url % tokens
        return url

    def _create_engine(self, url):
        echo = self.configuration.get('echo')
        if echo:
            echo = 'debug'

        engine = self.dialect.create_engine(url, self.schema, echo=echo)
        return engine, sessionmaker(bind=engine)

    def _get_migration_interface(self):
        migrations = self.configuration.get('migrations')
        if migrations:
            return MigrationInterface(self.schema, get_package_path(migrations))

class SchemaDependency(Dependency):
    def __init__(self, schema, **params):
        self.schema = schema
        super(SchemaDependency, self).__init__(SchemaInterface, 'schema:%s' % schema, **params)

    def contribute_params(self):
        return {'schema': self.schema}
