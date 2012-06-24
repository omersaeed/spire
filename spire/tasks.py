import os
from pprint import pformat

from bake import *
from scheme import *

from spire.support.task import SpireTask
from spire.schema.tasks import *

class DumpConfiguration(SpireTask):
    name = 'spire.dump-configuration'
    
    def run(self, runtime):
        self.prepare(runtime)
        runtime.report(pformat(self.assembly.configuration), True)

class StartShell(Task):
    name = 'spire.shell'
    description = 'starts a spire server using the python shell'
    parameters = {
        'config': Text(description='path to spire configuration', default='spire.yaml'),
        'ipython': Boolean(description='use ipython if available', default=True),
    }

    IPYTHON_CODE = '''"from spire.drivers.shell import Driver;Driver().configure('%s').deploy()"'''

    def run(self, runtime):
        ipython = self['ipython']
        if ipython:
            try:
                import IPython
            except ImportError:
                ipython = False

        if ipython:
            os.execvp('ipython', ['ipython', '-i', '-c', self.IPYTHON_CODE % self['config']])
        else:
            os.execvp('python', ['python', '-i', '-m', 'spire.drivers.shell', self['config']])

class StartWsgiServer(SpireTask):
    name = 'spire.wsgi'
    description = 'starts a spire server using the wsgi driver'
    parameters = {
        'address': Text(description='hostname:port', default='localhost:8000'),
    }

    def run(self, runtime):
        from spire.drivers.wsgi import Driver
        Driver(self['address'], self['config'])
