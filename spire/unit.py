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

        configuration = {}
        dependencies = {}

        for base in reversed(bases):
            inherited_configuration = getattr(base, 'configuration', None)
            if inherited_configuration:
                configuration.update(inherited_configuration.structure)

            inherited_dependencies = getattr(base, 'dependencies', None)
            if inherited_dependencies:
                dependencies.update(inherited_dependencies)

        declared_configuration = namespace.get('configuration')
        if declared_configuration:
            if isinstance(declared_configuration, Structure):
                declared_configuration = declared_configuration.structure
            configuration.update(declared_configuration)

        for attr, value in namespace.iteritems():
            if isinstance(value, Dependency):
                dependencies[attr] = value

        unit.dependencies = dependencies
        if configuration:
            unit.configuration = Structure(configuration)

        if not abstract:
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

    def assemble(self):
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

class Assembly(object):
    """A unit assembly."""

    def __init__(self, principal):
        self.cache = {}
        self.configuration = {}
        self.principal = principal
        self.root = None
        self.units = {}

    @property
    def schema(self):
        try:
            return self._constructed_schema
        except AttributeError:
            pass

        schema = {}
        queue = [(Dependency(None, self.principal), [])]
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

        self._constructed_schema = Structure(schema, nonnull=True)
        return self._constructed_schema

    def assemble(self, configuration=None):
        if configuration:
            self.configure(configuration)

        self.root = self.principal(None)
        units = [self.root]

        queue = [(self.root, [])]
        while queue:
            unit, tokens = queue.pop(0)
            for name, dependency in unit.dependencies.iteritems():
                if dependency.unit:
                    path = tokens + [name]
                    instance = self._assemble_unit(path, dependency)
                    if instance:
                        setattr(unit, name, instance)
                        units.insert(0, instance)
                        queue.append((instance, path))
                    else:
                        setattr(unit, name, None)

        assembled = set()
        for unit in units:
            if unit not in assembled:
                unit.assemble()
                assembled.add(unit)

    def collate(self, cls=None):
        collated = set()
        for unit in self.units.itervalues():
            if (cls is None or isinstance(unit, cls)) and unit not in collated:
                collated.add(unit)
                yield unit

    def configure(self, configuration):
        self.configuration.update(self.schema.process(configuration))
        return self

    def _assemble_unit(self, path, dependency):
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
