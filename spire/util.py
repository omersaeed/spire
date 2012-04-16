from inspect import getargspec

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
