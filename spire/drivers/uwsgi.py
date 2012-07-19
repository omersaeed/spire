from __future__ import absolute_import

import os
import sys
import threading
from traceback import extract_stack

from spire.drivers.driver import Driver
from spire.wsgi.util import Dispatcher, Mount

THREAD_DUMP_TRIGGER = '/tmp/uwsgi-dump-threads'
THREAD_DUMP_SIGNAL = 17

try:
    import uwsgi
except ImportError:
    uwsgi = None

class Driver(Driver):
    def __init__(self, assembly=None):
        super(Driver, self).__init__(assembly=assembly)
        for key in ('yaml', 'yml', 'json'):
            filename = uwsgi.opt.get(key)
            if filename:
                self.configure(filename)
                break
        else:
            raise RuntimeError()

        self.deploy()

        self.dispatcher = Dispatcher()
        for unit in self.assembly.collate(Mount):
            self.dispatcher.mount(unit.configuration['path'], unit)

    def __call__(self, environ, start_response):
        return self.dispatcher.dispatch(environ, start_response)

def dump_threads(*args):
    lines = ['*** begin thread dump']
    names = dict((t.ident, t.name) for t in threading.enumerate())
    for id, stack in sys._current_frames().items():
        line = 'Thread: %s' % id
        if id in names:
            line += ' "%s"' % names[id]
        lines.append(line)
        for filename, lineno, name, line in extract_stack(stack):
            lines.append('  File "%s", line %d, in %s' % (filename, lineno, name))
            if line:
                lines.append('    %s' % line.strip())

    lines.append('*** end thread dump')
    print '\n'.join(lines)

def register_thread_dumper():
    os.close(os.open(THREAD_DUMP_TRIGGER, os.O_WRONLY|os.O_CREAT, 0666))
    uwsgi.register_signal(THREAD_DUMP_SIGNAL, 'workers', dump_threads)
    uwsgi.add_file_monitor(THREAD_DUMP_SIGNAL, THREAD_DUMP_TRIGGER)

if uwsgi:
    register_thread_dumper()
    uwsgi.applications = {'': Driver()}
