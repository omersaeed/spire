from threading import RLock

from scheme import Structure
from spire.exceptions import *
from spire.util import identify_class, import_object

GLOBAL = 'global'
UNIT = 'unit'

class UnitCache(object):
    """The unit cache."""

    cache = {}
    guard = RLock()

    @classmethod
    def acquire(cls, identifier, instantiator):
        cls.guard.acquire()
        try:
            try:
                return cls.cache[identifier]
            except KeyError:
                instance = cls.cache[identifier] = instantiator()
                return instance
        finally:
            cls.guard.release()

class Dependency(object):
    """A dependency."""

    def __init__(self, token, unit, optional=False, scope=None):
        self.configurable = (unit.configuration is not None)
        self.hierarchy = None
        self.instance = None
        self.optional = optional
        self.satisfied = False
        self.scope = scope or GLOBAL
        self.token = token
        self.unit = unit

    def __get__(self, instance, owner):
        if instance is not None:
            return self.get()
        else:
            return self

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, self.token)

    def construct_configuration(self):
        return self.unit.configuration

    def get(self):
        if self.instance is not None:
            return self.instance

        if not self.satisfied:
            if self.optional:
                return None
            else:
                raise UnsatisfiedDependencyError(self.token)

        scope = self.scope
        if scope == UNIT:
            identifier = self
        elif scope == GLOBAL:
            identifier = self.token
        else:
            identifier = self.token
            for dependency in reversed(self.hierarchy):
                if dependency.unit.__name__.lower() == scope:
                    identifier = (dependency, self.token)
                    break

        self.instance = UnitCache.acquire(identifier, self.instantiate)
        return self.instance

    def instantiate(self):
        return self.unit(self.configuration)

    def satisfy(self, hierarchy, configuration, scope=None):
        self.configuration = configuration
        self.hierarchy = hierarchy
        self.satisfied = True
        if scope is not None:
            self.scope = scope

class UnitDependency(Dependency):
    """A unit dependency."""

    def __init__(self, unit, optional=False, scope=None):
        if isinstance(unit, basestring):
            unit = import_object(unit)
        if not (isinstance(unit, type) and issubclass(unit, Unit)):
            raise SpecificationError()

        token = 'unit:%s' % unit.__identity__
        super(UnitDependency, self).__init__(token, unit, optional, scope)

class UnitMeta(type):
    def __new__(metatype, name, bases, namespace):
        abstract = namespace.pop('__abstract__', False)
        unit = type.__new__(metatype, name, bases, namespace)
        if abstract:
            return unit

        if unit.configuration is not None:
            if isinstance(unit.configuration, dict):
                unit.configuration = Structure(unit.configuration)
            if not isinstance(unit.configuration, Structure):
                raise SpecificationError()

        unit.dependencies = {}
        for attr, value in namespace.iteritems():
            if isinstance(value, Dependency):
                unit.dependencies[attr] = value

        unit.__identity__ = identify_class(unit)
        unit.units[unit.__identity__] = unit
        return unit

class Unit(object):
    """A spire unit."""

    __metaclass__ = UnitMeta
    __abstract__ = True
    units = {}

    configuration = None

    def __new__(cls, configuration=None):
        instance = super(Unit, cls).__new__(cls)
        instance.configuration = configuration
        return instance

class Stack(object):
    """A unit stack."""

    def __init__(self, principal):
        self.principal = principal
        self.dependencies = self._collate_dependencies(principal)
        self.configuration = self._construct_configuration(self.dependencies)

    def configure(self, configuration):
        if isinstance(configuration, basestring):
            configuration = self.configuration.read(configuration)
        else:
            configuration = self.configuration.process(configuration)
        self._satisfy_dependencies(configuration)

    def deploy(self):
        return self.principal()

    def _collate_dependencies(self, unit):
        dependencies = {}
        queue = [([], [], Dependency(None, unit))]
        while queue:
            ancestors, tokens, subject = queue.pop(0)
            for name, dependency in subject.unit.dependencies.iteritems():
                if dependency.unit:
                    hierarchy = ancestors + [subject]
                    path = tokens + [name]
                    queue.append((hierarchy, path, dependency))
                    dependencies['.'.join(path)] = (dependency, hierarchy)
        return dependencies

    def _construct_configuration(self, dependencies):
        configuration = {}
        for name, (dependency, hierarchy) in dependencies.iteritems():
            schema = dependency.construct_configuration()
            if schema:
                configuration[dependency.token] = schema
                configuration[name] = schema.clone(required=False)
        return Structure(configuration, nonnull=True)

    def _satisfy_dependencies(self, configurations):
        dependencies = self.dependencies
        for token, configuration in configurations.iteritems():
            pair = dependencies.get(token)
            if pair:
                pair[0].satisfy(pair[1], configuration, UNIT)

        for dependency, hierarchy in dependencies.itervalues():
            if not dependency.satisfied:
                if dependency.configurable:
                    configuration = configurations.get(dependency.token)
                    if configuration:
                        dependency.satisfy(hierarchy, configuration)
                    elif not dependency.optional:
                        raise UnsatisfiedDependencyError(dependency.token)
                else:
                    dependency.satisfy(hierarchy, None)
