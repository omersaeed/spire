from scheme import *
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker

from spire.assembly import Configuration, Dependency, configured_property
from spire.unit import Unit

__all__ = ('SchemaInterface', 'SchemaDependency')

class SchemaInterface(Unit):
    configuration = Configuration({
        'echo': Boolean(default=False),
        'schema': Text(nonempty=True),
        'url': Text(nonempty=True),
    })

    schema = configured_property('schema')

    def __init__(self, schema, url, echo=False):
        self.engine = create_engine(url, echo=echo)
        self.session = sessionmaker(bind=self.engine)

class SchemaDependency(Dependency):
    def __init__(self, schema, **params):
        self.schema = schema
        super(SchemaDependency, self).__init__(SchemaInterface, 'schema:%s' % schema, **params)

    def contribute(self):
        return {'schema': self.schema}
