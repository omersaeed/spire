import sys
import os

from scheme import Format

from spire.component import Registry


class Driver(object):
    def __init__(self, configuration):
        if isinstance(configuration, basestring):
            configuration = Format.read(configuration)
        if 'spire' in configuration:
            configuration = configuration['spire']
        else:
            raise RuntimeError()

        Registry.configure(configuration)
        Registry.deploy()

if __name__ == '__main__':
    if os.getenv('PYTHONSTARTUP', False):
        if os.path.exists(os.environ['PYTHONSTARTUP']):
            execfile(os.environ['PYTHONSTARTUP'])
    Driver(sys.argv[1])
