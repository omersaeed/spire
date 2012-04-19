import os
import sys
from inspect import getargspec
from types import ModuleType

def call_with_supported_params(callable, *args, **params):
    arguments = getargspec(callable)[0]
    for key in params.keys():
        if key not in arguments:
            del params[key]
    return callable(*args, **params)

def enumerate_modules(package, import_modules=False):
    path = get_package_path(package)
    for filename in os.listdir(path):
        if filename[-3:] == '.py' and filename != '__init__.py':
            module = '%s.%s' % (package, filename[:-3])
            if import_modules:
                module = import_object(module)
            yield module

def get_constructor_args(cls, cache={}):
    try:
        return cache[cls]
    except KeyError:
        pass

    try:
        signature = getargspec(cls.__init__)
    except TypeError:
        arguments = []
    else:
        arguments = signature[0][1:]

    cache[cls] = arguments
    return arguments

def get_package_path(module, path=None):
    if isinstance(module, basestring):
        module = __import__(module, None, None, [module.split('.')[-1]])
    if not isinstance(module, list):
        module = module.__path__

    modulepath = module[0]
    for prefix in sys.path:
        if prefix in ('', '..'):
            prefix = os.getcwd()
        fullpath = os.path.abspath(os.path.join(prefix, modulepath))
        if os.path.exists(fullpath):
            break
    else:
        return None

    if path:
        fullpath = os.path.join(fullpath, path)
    return fullpath

def identify_class(cls):
    if cls.__module__ == '__main__':
        return cls.__name__
    return '%s.%s' % (cls.__module__, cls.__name__)

def import_object(path):
    attr = None
    if ':' in path:
        path, attr = path.split(':')
        return getattr(__import__(path, None, None, [attr]), attr)

    try:
        return __import__(path, None, None, [path.split('.')[-1]])
    except ImportError:
        if '.' in path:
            path, attr = path.rsplit('.', 1)
            return getattr(__import__(path, None, None, [attr]), attr)
        else:
            raise

def is_class(obj):
    return (isinstance(obj, object) and isinstance(obj, type))

def is_module(obj):
    return (isinstance(obj, ModuleType))

def is_package(obj):
    return (isinstance(obj, ModuleType) and obj.__name__ == obj.__package__)

def recursive_merge(original, addition):
    for key, value in addition.iteritems():
        if key in original:
            source = original[key]
            if isinstance(source, dict) and isinstance(value, dict):
                value = recursive_merge(source, value)
            original[key] = value
        else:
            original[key] = value
    return original
