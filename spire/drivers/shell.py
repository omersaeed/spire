import os
import sys

from spire.drivers.driver import Driver

if __name__ == '__main__':
    if os.getenv('PYTHONSTARTUP', False):
        if os.path.exists(os.environ['PYTHONSTARTUP']):
            execfile(os.environ['PYTHONSTARTUP'])
    driver = Driver().configure(sys.argv[1]).deploy()
