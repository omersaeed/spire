from threading import Lock

from sqlalchemy import Column, MetaData, Table, event
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm import mapper
from sqlalchemy.orm.mapper import Mapper
from sqlalchemy.orm.session import object_session

from spire.schema.schema import Schema, SessionLocals
from spire.util import get_constructor_args, pluralize

__all__ = ('Model',)

MAPPER_PARAMS = get_constructor_args(Mapper)

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

        abstract = False
        tablename = None

        meta = namespace.pop('meta', None)
        if meta:
            if not isinstance(meta, dict):
                meta = meta.__dict__

            if 'abstract' in meta:
                abstract = meta.pop('abstract')
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
        if '__tablename__' not in namespace and not abstract:
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

    @property
    def session(self):
        return object_session(self)

    def update_with_mapping(self, mapping=None, **params):
        cls = type(self)
        for attr, value in params.iteritems():
            setattr(self, attr, value)
        if mapping:
            for attr, value in mapping.iteritems():
                if attr not in params:
                    setattr(self, attr, value)
        return self

Model = declarative_base(cls=ModelBase, constructor=None, name='Model', metaclass=ModelMeta)
event.listen(mapper, 'mapper_configured', AttributeValidator._attach_validators)
