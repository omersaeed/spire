from spire.local import ContextLocals

class ContextLocalPurger(object):
    def __init__(self, application):
        self.application = application

    def __call__(self, environ, start_response):
        response = self.application(environ, start_response)
        ContextLocals.purge()
        return response
