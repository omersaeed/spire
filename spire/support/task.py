from bake import *

from spire.core import Assembly
from spire.drivers.driver import Driver

class SpireTask(Task):
    parameters = {
        'config': Path(description='path to spire configuration file', default=path('spire.yaml')),
    }

    @property
    def assembly(self):
        return Assembly.current()

    def prepare(self, runtime):
        config = self['config']
        if not config.exists():
            raise TaskError("configuration file '%s' does not exist" % config)

        self.driver = Driver(str(config))
