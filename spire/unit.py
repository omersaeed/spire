from threading import RLock

from scheme import Structure

from spire.exceptions import *
from spire.util import identify_class, import_object

GLOBAL = 'global'
UNIT = 'unit'

class Dependency(object):
    """A spire dependency."""

    def __init__(self, token, unit, optional=False, scope=None):
        self.configurable = (unit.configuration is not None)
        self.optional = optional
        self.scope = scope
        self.token = token
        self.unit = unit

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, self.token)

    def configuration(self):
        return self.unit.configuration

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
                raise SpireError()

        unit.dependencies = {}
        for attr, value in namespace.iteritems():
            if isinstance(value, Dependency):
                unit.dependencies[attr] = value

        unit.units[identify_class(unit)] = unit
        return unit

class Unit(object):
    """A spire unit."""

    __metaclass__ = UnitMeta
    __abstract__ = True

    configuration = None
    dependencies = None
    units = {}

    def __init__(self, configuration):
        self.configuration = configuration
        self.identities = set()

    def initialize(self):
        pass

    def _assume_identity(self, identity):
        self.identities.add(identity)

class UnitDependency(Dependency):
    """A unit dependency."""

    def __init__(self, unit, optional=False, scope=None):
        if isinstance(unit, basestring):
            unit = import_object(unit)
        if not (isinstance(unit, type) and issubclass(unit, Unit)):
            raise SpireError()

        token = 'unit:%s' % identify_class(unit)
        super(UnitDependency, self).__init__(token, unit, optional, scope)

class UnitStack(object):
    """A unit stack."""

    def __init__(self, principal):
        self.cache = {}
        self.configuration = {}
        self.principal = principal
        self.root = None
        self.schema = self._construct_schema(principal)
        self.units = {}

    def collate(self, contract):
        collated = set()
        for unit in self.units.itervalues():
            if isinstance(unit, contract) and unit not in collated:
                collated.add(unit)
                yield unit

    def configure(self, configuration):
        self.configuration.update(self.schema.process(configuration))
        return self

    def deploy(self):
        self.root = self.principal(None)
        units = [self.root]

        queue = [(self.root, [])]
        while queue:
            unit, tokens = queue.pop(0)
            for name, dependency in unit.dependencies.iteritems():
                if dependency.unit:
                    path = tokens + [name]
                    instance = self._deploy_unit(path, dependency)
                    if instance:
                        setattr(unit, name, instance)
                        units.insert(0, instance)
                        queue.append((instance, path))
                    else:
                        setattr(unit, name, None)

        initialized = set()
        for unit in units:
            if unit not in initialized:
                unit.initialize()
                initialized.add(unit)

    def _construct_schema(self, principal):
        schema = {}
        queue = [(Dependency(None, principal), [])]
        while queue:
            subject, tokens = queue.pop(0)
            for name, dependency in subject.unit.dependencies.iteritems():
                if dependency.unit:
                    path = tokens + [name]
                    queue.append((dependency, path))
                    configuration = dependency.configuration()
                    if configuration:
                        for key in ('.'.join(path), dependency.token):
                            schema[key] = configuration
        return Structure(schema, nonnull=True)

    def _deploy_unit(self, path, dependency):
        identity = '.'.join(path)
        if identity in self.configuration:
            instance = dependency.unit(self.configuration[identity])
            instance._assume_identity(identity)
            self.units[identity] = instance
            return instance

        token = dependency.token
        
        instance = self.cache.get(token)
        if instance is not None:
            self.units[identity] = instance
            instance._assume_identity(identity)
            return instance

        configuration = self.configuration.get(dependency.token)
        if not configuration:
            if dependency.optional:
                return None
            elif dependency.configurable:
                raise UnsatisfiedDependencyError(dependency.token)

        instance = dependency.unit(configuration)
        instance._assume_identity(identity)

        self.cache[token] = self.units[identity] = instance
        return instance
