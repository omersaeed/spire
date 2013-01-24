from __future__ import absolute_import

import os
import sys

from spire.runtime.runtime import Runtime
from spire.util import dump_threads
from spire.wsgi.util import Mount, MountDispatcher

IPYTHON_CONSOLE_TRIGGER = '/tmp/activate-%s-console'
IPYTHON_CONSOLE_SIGNAL = 18

try:
    import uwsgi
except ImportError:
    uwsgi = None

def activate_ipython_console(n):
    from IPython import embed_kernel
    embed_kernel(local_ns={'uwsgi': uwsgi})

class Runtime(Runtime):
    def __init__(self, configuration=None, assembly=None):
        super(Runtime, self).__init__(assembly=assembly)
        for key in ('yaml', 'yml', 'json'):
            filename = uwsgi.opt.get(key)
            if filename:
                self.configure(filename)
                break
        else:
            raise RuntimeError()

        self.deploy()
        self.startup()

        self.dispatcher = MountDispatcher()
        for unit in self.assembly.collate(Mount):
            self.dispatcher.mount(unit)

        name = self.parameters.get('name')
        if name:
            self.register_ipython_console(name)

    def __call__(self, environ, start_response):
        return self.dispatcher.dispatch(environ, start_response)

    def register_ipython_console(self, name):
        trigger = IPYTHON_CONSOLE_TRIGGER % name
        os.close(os.open(trigger, os.O_WRONLY|os.O_CREAT, 0666))

        uwsgi.register_signal(IPYTHON_CONSOLE_SIGNAL, 'mule', activate_ipython_console)
        uwsgi.add_file_monitor(IPYTHON_CONSOLE_SIGNAL, trigger)

    def reload(self):
        uwsgi.reload()

if uwsgi:
    uwsgi.applications = {'': Runtime()}
