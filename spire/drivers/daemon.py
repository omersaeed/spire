from scheme import *
from scheme.supplemental import ObjectReference

from spire.drivers.driver import Driver
from spire.support.daemon import *

Schema = Structure({
    'detached': Boolean(default=True),
    'gid': Text(nonnull=True),
    'pidfile': Text(nonnull=True),
    'uid': Text(nonnull=True),
})

class Driver(Driver):
    def __init__(self, configuration=None, assembly=None, **params):
        super(Driver, self).__init__(configuration, assembly)
        self.deploy()

        configuration = self.configuration.get('daemon') or {}
        configuration = Schema.process(configuration, serialized=True)

        for attr, value in params.iteritems():
            if value is not None:
                configuration[attr] = value

        self.daemon = self.assembly.collate(Daemon, single=True)
        if not self.daemon:
            raise Exception('no daemon component')

        if configuration['detached']:
            detach_process()

        self.pidfile = None
        if 'pidfile' in configuration:
            self.pidfile = Pidfile(configuration['pidfile'])
            self.pidfile.write()

        if 'uid' in configuration:
            switch_user(configuration['uid'], configuration.get('gid'))

        self.daemon.run()
