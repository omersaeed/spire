


class ContextParserMiddleware(object):
    def __init__(self, application, prefix='HTTP_X_SPIRE_', key='spire.context'):
        self.application = application
        self.key = key
        self.prefix = prefix
        self.prefix_length = len(prefix)

    def __call__(self, environ, start_response):
        self._parse_context(environ)
        return self.application(environ, start_response)
    
    def _parse_context(self, environ):
        prefix = self.prefix
        length = self.prefix_length

        context = environ[self.key] = {}
        for name, value in environ.iteritems():
            if name[:length] == prefix:
                context[name[length:].lower()] = value
