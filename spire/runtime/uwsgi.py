from __future__ import absolute_import

import os
import sys

from spire.runtime.runtime import Runtime
from spire.util import dump_threads
from spire.wsgi.util import Mount, MountDispatcher

THREAD_DUMP_TRIGGER = '/tmp/uwsgi-dump-threads'
THREAD_DUMP_SIGNAL = 17

try:
    import uwsgi
except ImportError:
    uwsgi = None

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

        self.register_thread_dumper()

    def __call__(self, environ, start_response):
        return self.dispatcher.dispatch(environ, start_response)

    def register_thread_dumper(self):
        os.close(os.open(THREAD_DUMP_TRIGGER, os.O_WRONLY|os.O_CREAT, 0666))
        uwsgi.register_signal(THREAD_DUMP_SIGNAL, 'workers',
            lambda *args: sys.stdout.write('%s\n' % dump_threads()))
        uwsgi.add_file_monitor(THREAD_DUMP_SIGNAL, THREAD_DUMP_TRIGGER)

if uwsgi:
    uwsgi.applications = {'': Runtime()}
