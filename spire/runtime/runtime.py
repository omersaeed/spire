from glob import glob
from threading import Lock

from scheme import Format, Sequence, Structure
from scheme.supplemental import ObjectReference

from spire.core import Assembly
from spire.support.logs import LogHelper, configure_logging
from spire.util import recursive_merge

COMPONENTS = Sequence(ObjectReference(nonnull=True), unique=True)

log = LogHelper('spire.runtime')

class Runtime(object):
    """A spire runtime."""

    guard = Lock()
    runtime = None

    def __new__(cls, *args, **params):
        with cls.guard:
            if cls.runtime:
                raise Exception('runtime already instantiated')

            cls.runtime = super(Runtime, cls).__new__(cls, *args, **params)
            return cls.runtime

    def __init__(self, configuration=None, assembly=None):
        self.assembly = assembly or Assembly.current()
        self.components = {}
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
            for pattern in includes:
                for include in sorted(glob(pattern)):
                    self.configure(include)

        recursive_merge(self.configuration, configuration)
        return self

    def deploy(self):
        configuration = self.configuration
        if 'logging' in configuration:
            configure_logging(configuration['logging'])

        components = configuration.get('components')
        if components:
            components = COMPONENTS.process(components, serialized=True)

        config = configuration.get('configuration')
        if config:
            self.assembly.configure(config)

        for component in components:
            self.components[component.identity] = self.assembly.instantiate(component)
        return self

    def startup(self):
        for component in self.components.itervalues():
            if hasattr(component, 'startup'):
                log('info', 'initiating startup of component %s', component.identity)
                component.startup()

def current_runtime():
    return Runtime.runtime
