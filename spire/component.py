from scheme import Sequence, Structure
from scheme.supplemental import ObjectReference

from spire.assembly import Assembly, Configurable, Dependency
from spire.exceptions import *
from spire.unit import Unit

__all__ = ('Component', 'Registry')

class Component(Unit, Configurable):
    pass

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

        configuration = configuration.get('configuration')
        if configuration:
            Assembly.configure_assembly(configuration)

    @classmethod
    def deploy(cls):
        units = cls.units
        if cls.components:
            for component in cls.components:
                units[component.identity] = component()
