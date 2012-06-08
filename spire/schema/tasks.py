from bake import *
from scheme import *

from spire.support.task import SpireTask

try:
    import alembic
except ImportError:
    alembic = None
else:
    from alembic import command

class CreateSchema(SpireTask):
    name = 'spire.schema.create'
    description = 'creates a spire schema within a database'
    parameters = {
        'incremental': Boolean(description='only create new tables', default=False),
        'schema': Text(description='name of the schema', required=True)
    }

    def run(self, runtime):
        from spire.schema import Schema
        interface = Schema.interface(self['schema'])

        if not self['incremental']:
            interface.drop_tables()

        interface.create_tables()

class AlembicTask(SpireTask):
    supported = bool(alembic)
    parameters = {
        'path': Path(description='path to migrations directory', default=path('alembic')),
    }

    @property
    def config(self):
        from alembic.config import Config
        config = Config()
        config.set_main_option('script_location', str(self['path']))
        config.config_file_name = str(self['path'] / 'alembic.ini')
        return config

class InitializeMigrations(AlembicTask):
    name = 'migration.init'
    description = 'initialize a migrations directory'

    def run(self, runtime):
        migrations = self['path']
        command.init(self.config, str(self['path']))

        (migrations / 'alembic.ini').unlink()
        (migrations / 'README').unlink()

class CreateRevision(AlembicTask):
    name = 'migration.create'
    description = 'create an migration'
    parameters = {
        'autogenerate': Boolean(description='autogenerate revision', default=False),
        'title': Text(description='short title for revision', required=True),
    }

    def run(self, runtime):
        command.revision(self.config, message=self['title'], autogenerate=self['autogenerate'])

class Downgrade(AlembicTask):
    name = 'migration.downgrade'
    description = 'downgrade to an older version of the schema'
    parameters = {
        'revision': Text(description='revision to downgrade to', default='head'),
        'sql': Boolean(description='generate sql instead of downgrading database', default=False),
    }

    def run(self, runtime):
        command.downgrade(self.config, revision=self['revision'], sql=self['sql'])

class Upgrade(AlembicTask):
    name = 'migration.upgrade'
    description = 'upgrade to an newer version of the schema'
    parameters = {
        'revision': Text(description='revision to upgrade to', default='head'),
        'sql': Boolean(description='generate sql instead of upgrading database', default=False),
    }

    def run(self, runtime):
        command.upgrade(self.config, revision=self['revision'], sql=self['sql'])

class ShowBranches(AlembicTask):
    name = 'migration.branches'
    description = 'show un-spliced branch points'

    def run(self, runtime):
        command.branches(self.config)

class ShowCurrent(AlembicTask):
    name = 'migration.current'
    description = 'show current migration revision'

    def run(self, runtime):
        command.current(self.config)

class ShowHistory(AlembicTask):
    name = 'migration.history'
    description = 'show changeset history'

    def run(self, runtime):
        command.history(self.config)
