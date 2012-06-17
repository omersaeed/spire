from threading import Lock

from sqlalchemy.orm.session import sessionmaker

from spire.schema.dialect import get_dialect

class EnginePool(object):
    """An engine pool."""

    def __init__(self, configuration):
        echo = configuration.get('echo')
        if echo:
            echo = 'debug'

        url = configuration['url']
        self.engine = get_dialect(url).create_engine_for_schema(url,
            configuration['schema'], echo=echo)
        self.session_maker = sessionmaker(bind=self.engine)

    def get_engine(self, **params):
        return self.engine

    def get_session(self, **params):
        return self.session_maker()

class FederatedEnginePool(EnginePool):
    """A federated engine pool."""

    def __init__(self, configuration):
        self.cache = {}
        self.configuration = configuration
        self.dialect = get_dialect(configuration['url'])
        self.guard = Lock()

    def get_engine(self, token=None):
        engine, session_maker = self._acquire_engine(token)
        return engine

    def get_session(self, token=None):
        engine, session_maker = self._acquire_engine(token)
        return session_maker()

    def _acquire_engine(self, token):
        try:
            return self.cache[token]
        except KeyError:
            pass

        self.guard.acquire()
        try:
            if token in self.cache:
                return self.cache[token]

            echo = self.configuration.get('echo')
            if echo:
                echo = 'debug'

            url = self.configuration['url'] + token
            engine = self.dialect.create_engine_for_schema(url, 
                self.configuration['schema'], echo=echo)
            session_maker = sessionmaker(bind=engine)

            self.cache[token] = (engine, session_maker)
            return engine, session_maker
        finally:
            self.guard.release()
