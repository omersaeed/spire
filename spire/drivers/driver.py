from scheme import Format, Sequence, Structure
from scheme.supplemental import ObjectReference

from spire.core import Assembly
from spire.support.logs import configure_logging
from spire.util import recursive_merge

class Driver(object):
    schema = Sequence(ObjectReference(nonnull=True), unique=True)

    def __init__(self, configuration=None, assembly=None):
        self.assembly = assembly or Assembly.current()
        self.components = None
        self.configuration = {}
        if configuration:
            self.configure(configuration)

    def configure(self, configuration, included=False):
        if isinstance(configuration, basestring):
            configuration = Format.read(configuration, quiet=True)
            if not configuration:
                return

        if 'logging' in configuration:
            configure_logging(configuration['logging'])

        if 'spire' in configuration:
            configuration = configuration['spire']

        if not included:
            components = configuration.get('components')
            if components:
                self.components = self.schema.process(components, serialized=True)

        includes = configuration.get('includes')
        if includes:
            for include in includes:
                self.configure(include, True)

        configuration = configuration.get('configuration')
        if configuration:
            recursive_merge(self.configuration, configuration)
        if included:
            return self

        self.assembly.configure(self.configuration)
        return self

    def deploy(self):
        if self.components:
            for component in self.components:
                self.assembly.instantiate(component)
        return self
