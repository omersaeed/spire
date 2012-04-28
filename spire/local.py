from threading import Lock

from werkzeug.local import LocalStack

from spire.exceptions import LocalError

class SubsetProxy(object):
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

class LocalManager(object):
    def __init__(self):
        self.guard = Lock()
        self.locals = {}

    def declare(self, token):
        self.guard.acquire()
        try:
            if token not in self.locals:
                self.locals[token] = LocalStack()
        finally:
            self.guard.release()

    def get(self, token):
        return self.locals[token].top

    def push(self, token, value):
        self.locals[token].push(value)
        return value

    def pop(self, token):
        return self.locals[token].pop()

    def require(self, token):
        value = self.locals[token].top
        if value is not None:
            return value
        else:
            raise LocalError(token)

    def subset_proxy(self, prefix):
        return SubsetProxy(self, prefix)

LOCAL = LocalManager()
