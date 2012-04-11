from spire.unit import Assembly, Dependency, Unit, UnitDependency

class Component(Unit):
    __abstract__ = True

class Container(Assembly):
    """A spire container."""

    def __init__(self, components):
        for name, component in components.items():
            if not isinstance(component, Dependency):
                components[name] = UnitDependency(component)

        principal = type('principal', (Unit,), components)
        super(Container, self).__init__(principal)
