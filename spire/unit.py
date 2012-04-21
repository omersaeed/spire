from copy import deepcopy
from threading import RLock

from scheme import Structure

from spire.assembly import *
from spire.exceptions import *
from spire.util import get_constructor_args, identify_object, import_object, recursive_merge

class UnitMeta(type):
    def __new__(metatype, name, bases, namespace):
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

        unit.identity = identify_object(unit)

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
                Assembly.register_dependency(value)
                unit.dependencies[attr] = value.register(unit, attr)

        for name, dependency in dependencies.iteritems():
            if name not in unit.dependencies:
                dependency = dependency.clone().register(unit, name)
                unit.dependencies[name] = dependency
                setattr(unit, name, dependency)

        Assembly.register_unit(unit.identity, unit)
        return unit

    def __call__(cls, *args, **params):
        identity = params.pop('__identity__', None)
        token = params.pop('__token__', None)

        if not token:
            token = identity = cls.identity

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
        unit.__identity__ = identity
        unit.__token__ = token

        print 'INSTANTIATING %s: token=%r, identity=%r' % (cls.identity, token, identity)

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

    configuration = None
    dependencies = None
    identity = None

    def collate_dependencies(self, cls=None):
        for dependency in self.dependencies.itervalues():
            if (cls is None or issubclass(dependency.unit, cls)):
                yield dependency.get(self)
