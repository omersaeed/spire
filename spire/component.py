from scheme import Sequence, Structure
from scheme.supplemental import ObjectReference

from spire.exceptions import *
from spire.unit import Assembly, Dependency, Unit

class Facet(Dependency):
    """A component facet."""

    def __init__(self, unit, optional=False, deferred=False, **params):
        super(Facet, self).__init__(unit, False, optional, deferred, **params)

class Component(Unit):
    abstract = True

class Registry(object):
    """The component registry."""

    components = None
    schema = Sequence(ObjectReference(nonnull=True), unique=True)
    units = {}

    @classmethod
    def collate(cls, superclass=None):
        for unit in cls.units.itervalues():
            for subunit in unit.collate_dependencies(superclass):
                yield subunit

    @classmethod
    def configure(cls, configuration):
        components = configuration.get('components')
        if components:
            cls.components = cls.schema.process(components, serialized=True)
        else:
            raise ConfigurationError()

        configuration = configuration.get('configuration')
        if configuration:
            Assembly.configure(configuration)
        else:
            raise ConfigurationError()

    @classmethod
    def deploy(cls):
        units = cls.units
        for component in cls.components:
            units[component.identity] = component()
