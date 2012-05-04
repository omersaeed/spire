from copy import deepcopy
from threading import RLock

from scheme import Structure, ValidationError

from spire.exceptions import *
from spire.util import identify_object, import_object, recursive_merge

__all__ = ('Assembly', 'Configurable', 'Configuration', 'Dependency', 'configured_property')

class Configurable(object):
    """A sentry class which indicates that subclasses can establish a configuration chain."""

class Assembly(object):
    """The spire assembly."""

    cache = {}
    classes = {}
    configuration = {}
    guard = RLock()
    schema = Structure({})

    @classmethod
    def acquire_unit(cls, token, instantiator, arguments):
        cls.guard.acquire()
        try:
            try:
                return cls.cache[token]
            except KeyError:
                print 'REGISTERING token: %s' % repr(token)
                instance = cls.cache[token] = instantiator(*arguments)
                return instance
        finally:
            cls.guard.release()

    @classmethod
    def configure_assembly(cls, configuration):
        configuration = cls.schema.process(configuration, serialized=True)
        recursive_merge(cls.configuration, configuration)

    @classmethod
    def is_configurable(cls, obj):
        return (obj is not Configurable and issubclass(obj, Configurable) and
            Configurable not in obj.__bases__)

    @classmethod
    def register_dependency(cls, dependency):
        token = dependency.token
        if not (dependency.configurable and token):
            return

        configuration = dependency.unit.configuration
        if token in cls.schema.structure:
            structure = cls.schema.structure[token]
            if configuration.required and not dependency.optional and not structure.required:
                structure.required = True
        else:
            schema = dependency.construct_schema(generic=True)
            if dependency.optional:
                schema = schema.clone(required=False)
            cls.schema.merge({token: schema})

    @classmethod
    def register_class(cls, identity, unit):
        cls.classes[identity] = unit
        if cls.is_configurable(unit):
            queue = [(unit, [identity], None)]
            while queue:
                subject, tokens, context = queue.pop(0)
                if subject.configuration:
                    if context:
                        structure = context.construct_schema()
                        if context.token and structure.required:
                            structure = structure.clone(required=False)
                    else:
                        structure = subject.configuration.schema.clone(required=False)
                    cls.schema.merge({'/'.join(tokens): structure})

                for attr, dependency in subject.dependencies.iteritems():
                    queue.append((dependency.unit, tokens + [attr], dependency))

    @classmethod
    def satisfy_dependency(cls, token, unit):
        cache_token = (token, None, type(unit))
        cls.guard.acquire()
        try:
            if cache_token not in cls.cache:
                cls.cache[cache_token] = unit
        finally:
            cls.guard.release()

    @classmethod
    def should_isolate(cls, identity):
        identity += '/'
        length = len(identity)

        for key in cls.configuration:
            if key[:length] == identity:
                return True
        else:
            return False

class Configuration(object):
    """A spire configuration."""

    def __init__(self, schema=None):
        schema = schema or {}
        if isinstance(schema, dict):
            schema = Structure(schema, nonnull=True)
        if not isinstance(schema, Structure):
            raise Exception()

        self.cache = {}
        self.schema = schema
        self.subject = None

    def __get__(self, instance, owner):
        if instance is not None:
            return self.get(instance)
        else:
            return self

    @property
    def required(self):
        return self.schema.has_required_fields

    def get(self, instance):
        try:
            return self.cache[instance]
        except KeyError:
            pass

        token = instance.__token__
        try:
            configuration = Assembly.configuration[token]
        except KeyError:
            configuration = self.schema.generate_default()

        self.cache[instance] = configuration
        return configuration

    def process(self, data, partial=False):
        return self.schema.process(data, serialized=True, partial=partial)

    def register(self, unit):
        self.subject = unit
        return self

class configured_property(object):
    """A property which delegates to a unit's configuration."""

    def __init__(self, key):
        self.key = key

    def __get__(self, instance, owner):
        if instance is not None:
            return instance.configuration[self.key]
        else:
            return self

class Dependency(object):
    """A spire dependency."""

    def __init__(self, unit, token=None, optional=False, deferred=True, **params):
        if token is None:
            token = unit.identity

        self.attr = None
        self.cache = {}
        self.deferred = deferred
        self.dependent = None
        self.optional = optional
        self.token = token
        self.unit = unit

        if params:
            if self.configurable:
                self.params = self.unit.configuration.process(params, partial=True)
            else:
                raise Exception()
        else:
            self.params = params

    def __get__(self, instance, owner):
        if instance is not None:
            return self.get(instance)
        else:
            return self

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, self.token)

    @property
    def configurable(self):
        return bool(self.unit.configuration)

    @property
    def configuration_required(self):
        return (self.unit.configuration and self.unit.configuration.required
            and not self.optional)

    def clone(self):
        dependency = deepcopy(self)
        dependency.attr = dependency.dependent = None
        dependency.cache = {}
        return dependency

    def construct_schema(self, generic=False):
        params = self.contribute()
        if not generic:
            params.update(self.params)

        configuration = self.unit.configuration
        if not params:
            required = configuration.schema.has_required_fields
            return configuration.schema.clone(required=required)

        fields = {}
        for name, field in configuration.schema.structure.iteritems():
            if name in params:
                fields[name] = field.clone(default=params[name], required=False)
            else:
                fields[name] = field

        structure = Structure(fields, nonnull=True)
        structure.required = structure.has_required_fields
        return structure

    def contribute(self):
        return {}

    def get(self, owner=None):
        try:
            return self.cache[owner]
        except KeyError:
            pass

        identity = None
        token = None
        if owner:
            identity = '%s/%s' % (owner.__identity__, self.attr)
            if identity in Assembly.configuration:
                token = identity

        if not token:
            if self.token:
                token = self.token
                if token not in Assembly.configuration and self.configuration_required:
                    raise ConfigurationError(token)
                if identity and not Assembly.should_isolate(identity):
                    identity = None
            else:
                token = identity

        cache_token = (token, identity, self.unit)
        print 'ACQUIRING: key=%r, token=%r, identity=%r' % (cache_token, token, identity)
        self.cache[owner] = Assembly.acquire_unit(cache_token, self.instantiate, (token, identity))
        return self.cache[owner]

    def instantiate(self, token, identity):
        params = self.contribute()
        params.update(self.params)
        return self.unit(__token__=token, __identity__=identity, **params)

    def register(self, unit, name):
        self.attr = name
        self.dependent = unit
        return self
