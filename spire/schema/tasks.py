from scheme import *

from spire.support.task import SpireTask

__all__ = ('CreateSchema',)

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
