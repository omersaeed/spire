from scheme import Structure

__all__ = ('Configurable', 'Registry')

class Configurable(object):
    """A sentry class which indicates that subclasses can establish a configuration chain."""

class Registry(object):
    """The unit registry."""

    schemas = {}
    units = {}

    @classmethod
    def is_configurable(cls, obj):
        return (obj is not Configurable and issubclass(obj, Configurable) and
            Configurable not in obj.__bases__)

    @classmethod
    def purge(cls):
        cls.schemas = {}
        cls.units = {}

    @classmethod
    def register_dependency(cls, dependency):
        token = dependency.token
        if not (dependency.configurable and token):
            return

        configuration = dependency.unit.configuration
        if token in cls.schemas:
            structure = cls.schemas[token]
            if configuration.required and not dependency.optional and not structure.required:
                structure.required = True
        else:
            schema = dependency.construct_schema(generic=True, name=token)
            if dependency.optional:
                schema = schema.clone(required=False)
            cls.schemas[token] = schema

    @classmethod
    def register_unit(cls, unit):
        cls.units[unit.identity] = unit
        if cls.is_configurable(unit):
            queue = [(unit, [unit.identity], None)]
            while queue:
                subject, tokens, dependency = queue.pop(0)
                if subject.configuration:
                    token = '/'.join(tokens)
                    if dependency:
                        structure = dependency.construct_schema(name=token)
                        if dependency.token and structure.required:
                            structure = structure.clone(required=False)
                    else:
                        structure = subject.configuration.schema.clone(required=False,
                            name=token)
                    cls.schemas[token] = structure

                for attr, subdependency in subject.dependencies.iteritems():
                    queue.append((subdependency.unit, tokens + [attr], subdependency))
