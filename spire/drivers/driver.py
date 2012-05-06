from scheme import Format, Sequence, Structure
from scheme.supplemental import ObjectReference

from spire.core import Assembly

class Driver(object):
    schema = Sequence(ObjectReference(nonnull=True), unique=True)

    def __init__(self, configuration=None, assembly=None):
        self.assembly = assembly or Assembly.current()
        self.components = None
        if configuration:
            self.configure(configuration)

    def configure(self, configuration):
        if isinstance(configuration, basestring):
            configuration = Format.read(configuration)
        if 'spire' in configuration:
            configuration = configuration['spire']

        components = configuration.get('components')
        if components:
            self.components = self.schema.process(components, serialized=True)

        configuration = configuration.get('configuration')
        if configuration:
            self.assembly.configure(configuration)

        return self

    def deploy(self):
        if self.components:
            for component in self.components:
                self.assembly.instantiate(component)
        return self
