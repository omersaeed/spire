from glob import glob
from threading import Lock
from time import sleep

from scheme import Boolean, Format, Integer, Sequence, Structure
from scheme.supplemental import ObjectReference

from spire.core import Assembly
from spire.exceptions import TemporaryStartupError
from spire.support.logs import LogHelper, configure_logging
from spire.util import recursive_merge

COMPONENTS_SCHEMA = Sequence(ObjectReference(nonnull=True), unique=True)
PARAMETERS_SCHEMA = Structure({
    'startup_attempts': Integer(default=12),
    'startup_enabled': Boolean(default=True),
    'startup_timeout': Integer(default=5),
})

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
        self.parameters = {}

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

        parameters = configuration.get('spire') or {}
        self.parameters = PARAMETERS_SCHEMA.process(parameters)

        components = configuration.get('components')
        if components:
            components = COMPONENTS_SCHEMA.process(components, serialized=True)

        config = configuration.get('configuration')
        if config:
            self.assembly.configure(config)

        for component in components:
            self.components[component.identity] = self.assembly.instantiate(component)
        return self

    def startup(self):
        if not self.parameters['startup_enabled']:
            log('warning', 'skipping startup of components')
            return

        attempts = self.parameters['startup_attempts']
        timeout = self.parameters['startup_timeout']

        for component in self.components.itervalues():
            if not hasattr(component, 'startup'):
                continue

            log('info', 'initiating startup of component %s', component.identity)
            for _ in range(attempts - 1):
                try:
                    component.startup()
                except TemporaryStartupError:
                    log('warning', 'startup of component %s delayed', component.identity)
                    sleep(timeout)
                except Exception:
                    log('exception', 'startup of component %s raised exception',
                        component.identity)
                    break
                else:
                    log('info', 'startup of component %s completed', component.identity)
                    break
            else:
                log('error', 'startup of component %s timed out', component.identity)

def current_runtime():
    return Runtime.runtime
