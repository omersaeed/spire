from bake import *
from scheme import Boolean

from spire.core import Assembly
from spire.runtime.runtime import Runtime

class SpireTask(Task):
    parameters = {
        'config': Path(description='path to spire configuration file', default=path('spire.yaml')),
        'configured': Boolean(hidden=True, default=False),
    }

    @property
    def assembly(self):
        return Assembly.current()

    def prepare(self, runtime):
        if self['configured']:
            return

        config = self['config']
        if not config.exists():
            raise TaskError("configure file '%s' does not exist" % config)

        self.runtime = Runtime(str(config))
        self.runtime.deploy()
