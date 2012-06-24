from alembic import command
from alembic.config import Config

class MigrationInterface(object):
    def __init__(self, schema, path):
        self.path = path
        self.schema = schema

        self.config = Config()
        self.config.set_main_option('script_location', path)

    def stamp(self, revision='head'):
        command.stamp(self.config, revision)

    def upgrade(self, revision='head'):
        command.upgrade(self.config, revision=revision)
