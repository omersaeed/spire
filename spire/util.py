import os
import re
import sys
from inspect import getargspec, stack
from types import ModuleType
from uuid import uuid4

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

def get_constructor_args(cls, ignore_private=True, cache={}):
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

    if ignore_private:
        arguments = [value for value in arguments if value[0] != '_']

    cache[cls] = arguments
    return arguments

def get_package_data(module, path=None):
    if path is None:
        if ':' in module:
            module, path = module.split(':', 1)
        else:
            raise ValueError(path)

    openfile = open(get_package_path(module, path))
    try:
        return openfile.read()
    finally:
        openfile.close()

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

def identify_object(obj, cache={}):
    if isinstance(obj, ModuleType):
        return obj.__name__
    elif isinstance(obj, object) and isinstance(obj, type):
        if obj.__module__ == '__main__':
            return obj.__name__
        return '%s.%s' % (obj.__module__, obj.__name__)

    try:
        return cache[obj]
    except KeyError:
        pass

    for name, module in sys.modules.iteritems():
        if module:
            for attr, value in module.__dict__.iteritems():
                if value is obj:
                    identity = cache[obj] = '%s.%s' % (name, attr)
                    return identity
    else:
        raise TypeError(obj)

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

PLURALIZATION_RULES = (
    (re.compile(r'ife$'), re.compile(r'ife$'), 'ives'),
    (re.compile(r'eau$'), re.compile(r'eau$'), 'eaux'),
    (re.compile(r'lf$'), re.compile(r'lf$'), 'lves'),
    (re.compile(r'[sxz]$'), re.compile(r'$'), 'es'),
    (re.compile(r'[^aeioudgkprt]h$'), re.compile(r'$'), 'es'),
    (re.compile(r'(qu|[^aeiou])y$'), re.compile(r'y$'), 'ies'),
)

def pluralize(word, quantity=None, rules=PLURALIZATION_RULES):
    if quantity == 1: 
        return word

    for pattern, target, replacement in rules:
        if pattern.search(word):
            return target.sub(replacement, word)
    else:
        return word + 's'

def pruned(mapping, *keys):
    pruned = {}
    for key, value in mapping.iteritems():
        if key not in keys:
            pruned[key] = value
    return pruned

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

VALID_CHARS_EXPR = re.compile(r'[^\w\s-]')
SPACER_EXPR = re.compile(r'[-_\s]+')

def slugify(value, spacer='-', lowercase=True):
    if not isinstance(value, unicode):
        value = unicode(value)

    value = value.encode('ascii', 'ignore')
    value = VALID_CHARS_EXPR.sub('', value).strip()

    if lowercase:
        value = value.lower()
    return SPACER_EXPR.sub(spacer, value)

def trace_stack(indent=''):
    lines = []
    for frame, filename, lineno, context, source, pos in reversed(stack()[1:]):
        lines.append('%sfile "%s", line %d, in %s' % (indent, filename, lineno, context))
        if source:
            lines.append('%s    %s' % (indent, source[0].strip()))
    return '\n'.join(lines)

def uniqid():
    return str(uuid4())
