from threading import Lock

from sqlalchemy import Column, MetaData, Table, event
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm import mapper
from sqlalchemy.orm.mapper import Mapper
from sqlalchemy.orm.session import object_session

from spire.local import LOCAL
from spire.util import get_constructor_args, pluralize

__all__ = ('Model',)

MAPPER_PARAMS = get_constructor_args(Mapper)
SESSIONS = LOCAL.subset_proxy('schema.session')

class Schema(object):
    guard = Lock()
    schemas = {}

    def __init__(self, name):
        self.metadata = MetaData()
        self.name = name

    @classmethod
    def register(cls, name):
        cls.guard.acquire()
        try:
            if name not in cls.schemas:
                cls.schemas[name] = cls(name)
                SESSIONS.declare(name)
            return cls.schemas[name]
        finally:
            cls.guard.release()

class AttributeValidator(object):
    def __init__(self, column):
        self.column = column

    def __call__(self, instance, value, oldvalue, initiator):
        column = self.column
        if value is None:
            if column.nullable:
                return value
            else:
                raise ValueError()
        else:
            return column.type.validate(instance, column, value) or value

    @staticmethod
    def _attach_validators(mapper, cls):
        for column_property in mapper.iterate_properties:
            if len(column_property.columns) == 1:
                column = column_property.columns[0]
                if hasattr(column.type, 'validate'):
                    event.listen(column_property.class_attribute, 'set',
                        AttributeValidator(column), retval=True)

class ModelMeta(DeclarativeMeta):
    def __new__(metatype, name, bases, namespace):
        schema = namespace['schema'] = Schema.register(namespace.get('schema'))
        namespace['metadata'] = schema.metadata
        tablename = None

        meta = namespace.pop('meta', None)
        if meta:
            if not isinstance(meta, dict):
                meta = meta.__dict__

            if 'tablename' in meta:
                tablename = meta.pop('tablename')

            mapper_params = {}
            for param in MAPPER_PARAMS:
                if param in meta:
                    mapper_params[param] = meta.pop(param)
            
            if mapper_params:
                namespace['__mapper_args__'] = mapper_params

            table_args = []
            for param in ('constraints', 'indexes'):
                if param in meta:
                    table_args.extend(meta.pop(param))

            if meta:
                meta = dict((key, value) for key, value in meta.iteritems() if key[0] != '_')
                table_args.append(meta)
            if table_args:
                namespace['__table_args__'] = tuple(table_args)

        if not tablename:
            tablename = pluralize(name.lower())
        if '__tablename__' not in namespace:
            namespace['__tablename__'] = tablename

        model = DeclarativeMeta.__new__(metatype, name, bases, namespace)
        return model

    def __call__(cls, *args, **params):
        instance = super(ModelMeta, cls).__call__(*args, **params)
        for column in instance.__table__.columns:
            default = column.default
            if default and getattr(instance, column.name) is None:
                if default.is_scalar:
                    setattr(instance, column.name, default.arg)
                elif default.is_callable:
                    setattr(instance, column.name, default.arg(None))

        session = SESSIONS.get(cls.schema.name)
        if session:
            session.add(instance)

        return instance

class ModelBase(object):
    def __init__(self, **params):
        cls = type(self)
        for attr, value in params.iteritems():
            if hasattr(cls, attr):
                setattr(self, attr, value)

    def __repr__(self):
        return "%s('%s')" % (type(self).__name__, unicode(self))

    def __unicode__(self):
        try:
            return self.id
        except AttributeError:
            return id(self)

    def delete(self):
        object_session(self).delete(self)

    @classmethod
    def get(cls, **filters):
        session = SESSIONS.require(cls.schema.name)
        return session.query(cls).filter_by(**filters).one()

    @classmethod
    def query(cls, *args, **params):
        session = SESSIONS.require(cls.schema.name)
        return session.query(cls, *args, **params)
    
Model = declarative_base(cls=ModelBase, constructor=None, name='Model', metaclass=ModelMeta)
event.listen(mapper, 'mapper_configured', AttributeValidator._attach_validators)
