from copy import deepcopy
from threading import RLock

from scheme import Structure

from spire.exceptions import *
from spire.util import get_constructor_args, identify_class, import_object, recursive_merge

class Assembly(object):
    """A spire assembly."""

    cache = {}
    configuration = {}
    guard = RLock()
    units = {}

    @classmethod
    def acquire(cls, token, instantiator):
        cls.guard.acquire()
        try:
            try:
                return cls.cache[token]
            except KeyError:
                instance = cls.cache[token] = instantiator(cls)
                return instance
        finally:
            cls.guard.release()

    @classmethod
    def configure(cls, configuration):
        recursive_merge(cls.configuration, cls.schema().process(configuration, serialized=True))

    @classmethod
    def schema(cls, reconstruct=False):
        if not reconstruct:
            try:
                return cls._constructed_schema
            except AttributeError:
                pass

        schema = {}
        tokens = {}

        for identity, unit in cls.units.iteritems():
            configuration = unit.configuration
            if configuration and not configuration.abstract:
                if configuration.required:
                    schema[identity] = configuration.schema.clone(required=True)
                else:
                    schema[identity] = configuration.schema

            for attr, dependency in unit.dependencies.iteritems():
                configuration = dependency.unit.configuration
                if not configuration:
                    continue

                token = dependency.token
                if token:
                    if token in tokens:
                        if not dependency.optional and not tokens[token][0]:
                            tokens[token][0] = True
                    else:
                        tokens[token] = [not dependency.optional, configuration]

                specific_schema = dependency.schema()
                for token in dependency.tokens:
                    if token != dependency.token:
                        schema[token] = specific_schema

        for token, (required, configuration) in tokens.iteritems():
            if required and configuration.required:
                schema[token] = configuration.schema.clone(required=True)
            else:
                schema[token] = configuration.schema

        cls._constructed_schema = Structure(schema, nonnull=True, strict=False)
        return cls._constructed_schema

class Configuration(object):
    """A spire configuration."""

    def __init__(self, schema=None, abstract=False):
        self.abstract = abstract
        self.cache = {}
        self.schema = Structure(schema or {}, nonnull=True)
        self.subject = None

    def __get__(self, instance, owner):
        if instance is not None:
            return self.get(instance)
        else:
            return self

    @property
    def required(self):
        for field in self.schema.structure.itervalues():
            if field.required:
                return True
        else:
            return False

    def get(self, instance):
        try:
            return self.cache[instance]
        except KeyError:
            pass

        token = getattr(instance, '__token__', instance.identity)
        try:
            configuration = Assembly.configuration[token]
        except KeyError:
            try:
                configuration = self.schema.process({})
            except ValidationError:
                raise ConfigurationError(token)

        self.cache[instance] = configuration
        return configuration

    def register(self, unit):
        self.subject = unit
        return self

class Dependency(object):
    """A spire dependency."""

    def __init__(self, unit, token=None, optional=False, deferred=True, **params):
        if token is None:
            token = unit.identity

        self.attr = None
        self.deferred = deferred
        self.dependent = None
        self.instance = None
        self.optional = optional
        self.params = params
        self.token = token
        self.unit = unit

    def __get__(self, instance, owner):
        if instance is not None:
            return self.get(instance)
        else:
            return self

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, self.token)

    @property
    def configured_token(self):
        try:
            return self._configured_token
        except AttributeError:
            pass

        for candidate in self.tokens:
            if candidate in Assembly.configuration:
                self._configured_token = candidate
                return candidate
        
        self._configured_token = None
        return None

    @property
    def tokens(self):
        tokens = ['%s+%s' % (self.dependent.identity, self.attr)]
        if self.token:
            tokens.extend(['%s+%s' % (self.dependent.identity, self.token), self.token])
        return tokens

    def clone(self):
        dependency = deepcopy(self)
        dependency.attr = dependency.dependent = None
        return dependency

    def get(self, instance):
        if self.instance is not None:
            return self.instance

        token = self.configured_token
        if not token:
            if self.unit.configuration and self.unit.configuration.required:
                raise ConfigurationError(self.unit.identity)
            else:
                token = self.unit

        self.instance = Assembly.acquire(token, self.instantiate)
        return self.instance

    def instantiate(self, assembly):
        return self.unit(__token__=self.configured_token, **self.params)

    def register(self, unit, name):
        self.attr = name
        self.dependent = unit
        return self

    def schema(self):
        schema = self.unit.configuration.schema
        if not self.params:
            return schema

        fields = {}
        for name, field in schema.structure.iteritems():
            if field.required and name in self.params:
                fields[name] = field.clone(required=False)
            else:
                fields[name] = field

        return Structure(fields, nonnull=True)

class UnitMeta(type):
    def __new__(metatype, name, bases, namespace):
        abstract = namespace.pop('abstract', False)

        additional = None
        for base in reversed(bases):
            metaclass = getattr(base, '__metaclass__', None)
            if metaclass and metaclass is not metatype:
                if not additional:
                    additional = metaclass
                elif metaclass is not additional:
                    raise SpireError()

        unit = None
        if additional:
            metatype = type('metaclass', (metatype, additional), {})
            unit = additional.__new__(metatype, name, bases, namespace)
        else:
            unit = type.__new__(metatype, name, bases, namespace)

        try:
            identify = unit.identify_unit
        except AttributeError:
            unit.identity = identify_class(unit)
        else:
            unit.identity = identify()

        configuration = {}
        dependencies = {}

        for base in reversed(bases):
            inherited_configuration = getattr(base, 'configuration', None)
            if isinstance(inherited_configuration, Configuration):
                configuration.update(inherited_configuration.schema.structure)

            inherited_dependencies = getattr(base, 'dependencies', None)
            if inherited_dependencies:
                dependencies.update(inherited_dependencies)

        declared = namespace.get('configuration')
        if isinstance(declared, Configuration):
            declared.schema.merge(configuration)
            declared.register(unit)
        else:
            if isinstance(declared, dict):
                configuration.update(declared)
            if configuration:
                unit.configuration = Configuration(configuration).register(unit)

        unit.dependencies = {}
        for attr, value in namespace.iteritems():
            if isinstance(value, Dependency):
                unit.dependencies[attr] = value.register(unit, attr)

        for name, dependency in dependencies.iteritems():
            if name not in unit.dependencies:
                dependency = dependency.clone().register(unit, name)
                unit.dependencies[name] = dependency
                setattr(unit, name, dependency)

        if abstract:
            if unit.configuration:
                unit.configuration.abstract = True
        else:
            Assembly.units[unit.identity] = unit

        return unit

    def __call__(cls, *args, **params):
        token = params.pop('__token__', None)

        signature = get_constructor_args(cls)
        if args:
            for i, argument in enumerate(args):
                try:
                    name = signature[i]
                    if name not in params:
                        params[name] = argument
                    else:
                        raise TypeError('duplicate arguments')
                except IndexError:
                    raise TypeError('too many arguments')

        unit = cls.__new__(cls)
        if token:
            unit.__token__ = token

        if cls.configuration:
            configuration = cls.configuration.get(unit)
            if params:
                for name, value in params.items():
                    if name not in configuration:
                        configuration[name] = value
                    if name not in signature:
                        del params[name]

            for name in signature:
                if name in configuration:
                    params[name] = configuration[name]

        for dependency in cls.dependencies.itervalues():
            if not dependency.deferred:
                dependency.get(unit)

        unit.__init__(**params)
        return unit

class Unit(object):
    """A spire unit."""

    __metaclass__ = UnitMeta
    abstract = True

    configuration = None
    dependencies = None
    identity = None

    def collate_dependencies(self, cls=None):
        for dependency in self.dependencies.itervalues():
            if (cls is None or issubclass(dependency.unit, cls)):
                yield dependency.get(self)
