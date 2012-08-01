import os
from pprint import pformat

from bake import *
from scheme import *

from spire.runtime import current_runtime
from spire.support.task import SpireTask
from spire.schema.tasks import *

class DeployComponent(SpireTask):
    name = 'spire.component.deploy'
    description = 'deploys a spire component'
    parameters = {
        'component': Text(nonempty=True),
    }
    
    def run(self, runtime):
        try:
            component = self.runtime.components[self['component']]
        except KeyError:
            raise TaskError('invalid component')
        else:
            component.deploy()

class DumpConfiguration(SpireTask):
    name = 'spire.dump-configuration'
    
    def run(self, runtime):
        self.prepare(runtime)
        self.driver.deploy()
        runtime.report(pformat(self.assembly.configuration), True)

class StartDaemon(Task):
    name = 'spire.daemon'
    description = 'starts a spire server using the daemon driver'
    parameters = {
        'config': Path(description='path to spire configuration file', default=path('spire.yaml')),
        'detached': Boolean(),
    }

    def run(self, runtime):
        from spire.runtime.daemon import Runtime
        Runtime(self['config'], detached=self['detached'])

class StartShell(Task):
    name = 'spire.shell'
    description = 'starts a spire server using the python shell'
    parameters = {
        'config': Text(description='path to spire configuration', default='spire.yaml'),
        'ipython': Boolean(description='use ipython if available', default=True),
    }

    IPYTHON_CODE = '''"from spire.runtime.shell import Runtime;Runtime().configure('%s').deploy()"'''

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
            os.execvp('python', ['python', '-i', '-m', 'spire.runtime.shell', self['config']])

class StartWsgiServer(Task):
    name = 'spire.wsgi'
    description = 'starts a spire server using the wsgi driver'
    parameters = {
        'address': Text(description='hostname:port', default='localhost:8000'),
        'config': Path(description='path to spire configuration file', default=path('spire.yaml')),
    }

    def run(self, runtime):
        from spire.runtime.wsgi import Runtime
        Runtime(self['address'], self['config'])
