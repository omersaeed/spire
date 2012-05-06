from threading import RLock, local

from spire.core.registry import Registry
from spire.exceptions import *
from spire.util import recursive_merge

__all__ = ('Assembly',)

class Local(local):
    assembly = None

class Assembly(object):
    """A spire assembly."""

    local = Local()
    standard = None

    def __init__(self):
        self.cache = {}
        self.configuration = {}
        self.guard = RLock()
        self.principals = {}

    def __enter__(self):
        self.promote()

    def __exit__(self, *args):
        self.demote()

    def __repr__(self):
        return 'Assembly(0x%08x)' % id(self)

    def acquire(self, key, instantiator, arguments):
        self.guard.acquire()
        try:
            try:
                return self.cache[key]
            except KeyError:
                instance = self.cache[key] = instantiator(*arguments)
                return instance
        finally:
            self.guard.release()

    def collate(self, superclass):
        units = set()
        for unit in self.cache.values():
            if isinstance(unit, superclass):
                units.add(unit)
            for dependency in unit.dependencies.itervalues():
                if issubclass(dependency.unit, superclass):
                    units.add(dependency.get(unit))
        return units

    @classmethod
    def current(cls):
        return cls.local.assembly or cls.standard

    def configure(self, configuration):
        for token, data in configuration.iteritems():
            schema = Registry.schemas.get(token)
            if schema:
                data = schema.process(data, serialized=True)
                recursive_merge(self.configuration, {token: data})

    def demote(self):
        if self.local.assembly is self:
            self.local.assembly = None
        return self

    def instantiate(self, unit):
        return self.acquire(unit.identity, unit, ())

    def should_isolate(self, identity):
        identity += '/'
        length = len(identity)

        for key in self.configuration:
            if key[:length] == identity:
                return True
        else:
            return False

    def promote(self):
        self.local.assembly = self
        return self

Assembly.standard = Assembly()
