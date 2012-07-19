from scheme import Format, Sequence, Structure
from scheme.supplemental import ObjectReference

from spire.core import Assembly
from spire.support.logs import configure_logging
from spire.util import recursive_merge

class Driver(object):
    schema = Sequence(ObjectReference(nonnull=True), unique=True)

    def __init__(self, configuration=None, assembly=None):
        self.assembly = assembly or Assembly.current()
        self.components = []

        self.configuration = {}
        if configuration:
            self.configure(configuration)

    def configure(self, configuration):
        if isinstance(configuration, basestring):
            configuration = Format.read(configuration, quiet=True)
            if not configuration:
                return

        includes = configuration.pop('include', None)
        if includes:
            for include in includes:
                self.configure(include)

        recursive_merge(self.configuration, configuration)
        return self

    def deploy(self):
        configuration = self.configuration
        if 'logging' in configuration:
            configure_logging(configuration['logging'])

        components = configuration.get('components')
        if components:
            self.components = self.schema.process(components, serialized=True)

        configuration = configuration.get('configuration')
        if configuration:
            self.assembly.configure(configuration)

        for component in self.components:
            self.assembly.instantiate(component)
        return self
