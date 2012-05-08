from bake import *

from spire.drivers.driver import Driver

class SpireTask(Task):
    parameters = {
        'config': Path(description='path to configuration file', required=True)
    }

    def prepare(self, runtime):
        config = self['config']
        if not config.exists():
            raise TaskError("configuration file '%s' does not exist" % config)

        self['driver'] = Driver(str(config))
