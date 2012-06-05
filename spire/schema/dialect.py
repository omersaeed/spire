import re

from sqlalchemy import create_engine, event
from sqlalchemy.engine.url import make_url

class Dialect(object):
    def __init__(self, dialect):
        self.dialect = dialect

    def create_database(self, url, **params):
        pass

    def create_engine_for_schema(self, url, schema, echo=False):
        return create_engine(url, echo=echo)

    def create_role(self, url, name, **params):
        pass

    def create_schema(self, url, name, **params):
        pass

    def drop_role(self, url, name, **params):
        pass

    def drop_schema(self, url, name, **params):
        pass

class PostgresqlDialect(Dialect):
    def create_database(self, url, owner=None, extant_database='postgres'):
        url, dbname = url.rsplit('/', 1)
        connection = self._get_connection('%s/%s' % (url, extant_database))

        sql = 'create database %s' % validate_sql_identifier(dbname)
        if owner:
            sql += ' owner %s' % validate_sql_identifier(owner)

        cursor = connection.cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close()

    def create_engine_for_schema(self, url, schema, echo=False):
        def listener(connection, record, proxy):
            cursor = connection.cursor()
            try:
                cursor.execute('set search_path to %s', [schema])
            finally:
                cursor.close()

        engine = create_engine(url, echo=echo)
        event.listen(engine, 'checkout', listener)
        return engine

    def create_role(self, url, name, login=True, superuser=False):
        sql = ['create role %s' % validate_sql_identifier(name)]
        if login:
            sql.append('login')
        if superuser:
            sql.append('superuser')

        self._execute_statement(url, sql)

    def create_schema(self, url, name, owner=None):
        sql = 'create schema %s' % validate_sql_identifier(name)
        if owner:
            sql += ' authorization %s' % validate_sql_identifier(owner)

        self._execute_statement(url, sql)

    def drop_role(self, url, name, if_exists=True):
        sql = ['drop role']
        if if_exists:
            sql.append('if exists')

        sql.append(validate_sql_identifier(name))
        self._execute_statement(url, sql)

    def drop_schema(self, url, name, cascade=False, if_exists=True):
        sql = ['drop schema']
        if if_exists:
            sql.append('if exists')

        sql.append(validate_sql_identifier(name))
        if cascade:
            sql.append('cascade')

        self._execute_statement(url, sql)

    def _execute_statement(self, url, sql):
        if isinstance(sql, list):
            sql = ' '.join(sql)

        cursor = self._get_connection(url).cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close()

    def _get_connection(self, url, autocommit=True):
        params = make_url(url).translate_connect_args(username='user')
        connection = self.dialect.dbapi().connect(**params)
        connection.autocommit = autocommit
        return connection

class SqliteDialect(Dialect):
    def create_engine_for_schema(self, url, schema, echo=False):
        engine = create_engine(url, echo=echo)

        @event.listens_for(engine, 'connect')
        def handle_checkout(connection, record):
            connection.isolation_level = None

        @event.listens_for(engine, 'begin')
        def handle_begin(connection):
            connection.execute('begin')

        return engine

DIALECTS = {
    ('postgresql', 'psycopg2'): PostgresqlDialect,
    ('sqlite', 'pysqlite'): SqliteDialect,
}

def get_dialect(url):
    dialect = make_url(url).get_dialect()
    implementation = DIALECTS[(dialect.name, dialect.driver)]
    return implementation(dialect)

def validate_sql_identifier(value):
    if re.match(r'^[_a-zA-Z][_a-zA-Z0-9]*$', value):
        return value
    else:
        raise ValueError(value)
