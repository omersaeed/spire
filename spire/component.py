from spire.unit import Unit, UnitMeta

class Service(Unit):
    """A spire service."""

    __abstract__ = True

class ComponentMeta(UnitMeta):
    def __new__(metatype, name, bases, namespace):
        component = UnitMeta.__new__(metatype, name, bases, namespace)
        if not component.name:
            return component

        component.components[component.name] = component
        return component

class Component(Unit):
    """A spire component."""

    __metaclass__ = ComponentMeta
    __abstract__ = True
    components = {}

    name = None
