from threading import Lock

from werkzeug.local import LocalStack, release_local

from spire.exceptions import LocalError

class StackProxy(object):
    def __init__(self, manager, token):
        self.manager = manager
        self.token = token

    def get(self, default=None):
        return self.manager.get(self.token, default)

    def push(self, value):
        return self.manager.push(self.token, value)

    def pop(self):
        return self.manager.pop(self.token)

    def require(self):
        return self.manager.require(self.token)

class PrefixedProxy(object):
    def __init__(self, manager, prefix):
        self.manager = manager
        self.prefix = [prefix]

    def declare(self, token):
        return self.manager.declare(self._apply_prefix(token))

    def get(self, token):
        return self.manager.get(self._apply_prefix(token))

    def push(self, token, value):
        return self.manager.push(self._apply_prefix(token), value)

    def pop(self, token):
        return self.manager.pop(self._apply_prefix(token))

    def require(self, token):
        return self.manager.require(self._apply_prefix(token))

    def _apply_prefix(self, token):
        if isinstance(token, tuple):
            token = list(token)
        else:
            token = [token]
        return tuple(self.prefix + token)

class ContextLocalManager(object):
    def __init__(self):
        self.guard = Lock()
        self.locals = {}

    def create_prefixed_proxy(self, prefix):
        return PrefixedProxy(self, prefix)

    def declare(self, token):
        self.guard.acquire()
        try:
            if token not in self.locals:
                self.locals[token] = LocalStack()
            return StackProxy(self, token)
        finally:
            self.guard.release()

    def get(self, token, default=None):
        value = self.locals[token].top
        if value is not None:
            return value
        else:
            return default

    def push(self, token, value):
        self.locals[token].push(value)
        return value

    def pop(self, token):
        return self.locals[token].pop()

    def purge(self):
        for stack in self.locals.itervalues():
            release_local(stack)

    def require(self, token):
        value = self.locals[token].top
        if value is not None:
            return value
        else:
            raise LocalError(token)

ContextLocals = ContextLocalManager()
