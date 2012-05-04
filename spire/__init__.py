from spire.assembly import *
from spire.component import *
from spire.unit import *

def bootstrap(configuration, deploy=True):
    from scheme import Format
    if isinstance(configuration, basestring):
        configuration = Format.read(configuration)
    if 'spire' in configuration:
        configuration = configuration['spire']

    Registry.configure(configuration)
    if deploy:
        Registry.deploy()
