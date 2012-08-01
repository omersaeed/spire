import os
import sys

from spire.runtime.runtime import Runtime

if __name__ == '__main__':
    if os.getenv('PYTHONSTARTUP', False):
        if os.path.exists(os.environ['PYTHONSTARTUP']):
            execfile(os.environ['PYTHONSTARTUP'])
    Runtime().configure(sys.argv[1]).deploy()
