from scheme import *
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker

from spire.assembly import Configuration, Dependency, configured_property
from spire.schema.model import Schema, SchemaInterface, SessionLocals
from spire.unit import Unit

__all__ = ('SchemaInterface', 'SchemaDependency')

class SchemaInterface(Unit, SchemaInterface):
    configuration = Configuration({
        'echo': Boolean(default=False),
        'schema': Text(nonempty=True),
        'url': Text(nonempty=True),
    })

    def __init__(self, schema, url, echo=False):
        super(SchemaInterface, self).__init__(schema, url, echo=echo)

class SchemaDependency(Dependency):
    def __init__(self, schema, **params):
        self.schema = schema
        super(SchemaDependency, self).__init__(SchemaInterface, 'schema:%s' % schema, **params)

    def contribute(self):
        return {'schema': self.schema}
