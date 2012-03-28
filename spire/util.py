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
