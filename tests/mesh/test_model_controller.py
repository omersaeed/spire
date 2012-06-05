from unittest2 import TestCase

import scheme
from mesh.standard import *
from mesh.transport.base import ServerResponse

from spire.core import *
from spire.local import ContextLocals
from spire.mesh.controllers import ModelController
from spire import schema as _schema
from spire.util import uniqid

class Example(_schema.Model):
    class meta:
        schema = 'example'

    id = _schema.UUID(nullable=False, primary_key=True, default=uniqid)
    name = _schema.Token(nullable=False)
    value = _schema.Integer()

class ExampleResource(Resource):
    name = 'example'
    version = 1

    class schema:
        id = scheme.UUID(nonempty=True)
        name = scheme.Token(nonempty=True)
        value = scheme.Integer()

class Controller(ModelController):
    resource = ExampleResource
    version = (1, 0)

    model = Example
    schema = _schema.SchemaDependency('example')
    mapping = {'id': 'id', 'name': 'name', 'value': 'value'}

class ModelControllerTestCase(TestCase):
    def assert_total(self, response, expected):
        self.assertIn('total', response.content)
        self.assertEqual(response.content['total'], expected)

    def assert_values(self, response, attr, expected, ordered=False):
        if not ordered and not isinstance(expected, set):
            expected = set(expected)

        self.assertIn('resources', response.content)
        values = [r[attr] for r in response.content['resources']]
        if not ordered:
            values = set(values)
        self.assertEqual(values, expected)

    def setUp(self):
        self.assembly = Assembly().promote()
        self.assembly.configure({
            'schema:example': {'url': 'sqlite:////tmp/ex.db'}
        })

        self.interface = _schema.Schema.interface('example')
        self.interface.create_tables()

        try:
            self._create_examples()
        except AttributeError:
            pass

    def tearDown(self):
        self.interface.drop_tables()
        self.assembly.demote()
        del self.assembly, self.interface

    def _execute_operation(self, request, subject=None, data=None):
        response = ServerResponse()
        controller = Controller()

        content = getattr(controller, request)(None, response, subject, data)
        if content and content is not response:
            response(content)
        if not response.status:
            response.status = OK

        ContextLocals.purge()
        return response

    def _execute_query(self, **filters):
        query = {}
        for key, value in filters.items():
            if key.startswith(('id', 'name', 'value')):
                query[key] = value
                del filters[key]

        if query:
            filters['query'] = query
        return self._execute_operation('query', data=filters)

class TestCRUD(ModelControllerTestCase):
    def _create_example(self, name='alpha', value=1):
        response = self._execute_operation('create', data={'name': name, 'value': value})
        self.assertEqual(response.status, OK)
        self.assertIsInstance(response.content, dict)
        self.assertIn('id', response.content)
        return response.content['id']

    def test_create(self):
        id = self._create_example()
        self.assertTrue(id)

class TestQuerySorting(ModelControllerTestCase):
    EXAMPLES = [('alpha', 1), ('alpha', 2), ('beta', 1), ('gamma', 3)]

    def _create_examples(self):
        examples = []
        for name, value in self.EXAMPLES:
            examples.append({'id': uniqid(), 'name': name, 'value': value})

        with self.interface.get_engine().begin() as connection:
            connection.execute(Example.__table__.insert(), *examples)

    def test_without_sorting(self):
        response = self._execute_query()
        self.assert_total(response, 4)

    def test_ascending_sort(self):
        response = self._execute_query(sort=['name+'])
        self.assert_total(response, 4)
        self.assert_values(response, 'name', ['alpha', 'alpha', 'beta', 'gamma'], True)

    def test_descending_sort(self):
        response = self._execute_query(sort=['name-'])
        self.assert_total(response, 4)
        self.assert_values(response, 'name', ['gamma', 'beta', 'alpha', 'alpha'], True)

    def test_multi_column_sort(self):
        response = self._execute_query(sort=['value+', 'name-'])
        self.assert_total(response, 4)
        self.assert_values(response, 'name', ['beta', 'alpha', 'alpha', 'gamma'])

class TestQueryOperatiors(ModelControllerTestCase):
    NAMES = 'alpha-one alpha-two beta-one gamma-one delta'

    def _create_examples(self):
        examples = []
        for i, name in enumerate(self.NAMES.split(' ')):
            examples.append({'id': uniqid(), 'name': name, 'value': i})

        with self.interface.get_engine().begin() as connection:
            connection.execute(Example.__table__.insert(), *examples)

    def test_simple_query(self):
        response = self._execute_operation('query')
        self.assertIsInstance(response.content, dict)
        self.assertIn('total', response.content)
        self.assertEqual(response.content['total'], 5)
        self.assertIn('resources', response.content)
        self.assertEqual(len(response.content['resources']), 5)

    def test_equal_operator(self):
        response = self._execute_query(name='alpha-one')
        self.assert_total(response, 1)
        self.assert_values(response, 'name', ['alpha-one'])

        response = self._execute_query(name='bad-name')
        self.assert_total(response, 0)

        response = self._execute_query(value=2)
        self.assert_total(response, 1)
        self.assert_values(response, 'value', [2])

        response = self._execute_query(name__equal='alpha-one')
        self.assert_total(response, 1)
        self.assert_values(response, 'name', ['alpha-one'])

    def test_not_operator(self):
        response = self._execute_query(name__not='alpha-one')
        self.assert_total(response, 4)
        self.assert_values(response, 'name', ['alpha-two', 'beta-one', 'gamma-one', 'delta'])

        response = self._execute_query(name__not='bad-name')
        self.assert_total(response, 5)

    def test_prefix_operator(self):
        response = self._execute_query(name__prefix='alpha')
        self.assert_total(response, 2)
        self.assert_values(response, 'name', ['alpha-one', 'alpha-two'])

        response = self._execute_query(name__prefix='bad')
        self.assert_total(response, 0)

    def test_suffix_operator(self):
        response = self._execute_query(name__suffix='one')
        self.assert_total(response, 3)
        self.assert_values(response, 'name', ['alpha-one', 'beta-one', 'gamma-one'])

        response = self._execute_query(name__suffix='bad')
        self.assert_total(response, 0)

    def test_contains_operator(self):
        response = self._execute_query(name__contains='-')
        self.assert_total(response, 4)
        self.assert_values(response, 'name', ['alpha-one', 'alpha-two', 'beta-one', 'gamma-one'])

        response = self._execute_query(name__contains='!')
        self.assert_total(response, 0)

    def test_gt_operator(self):
        response = self._execute_query(value__gt=2)
        self.assert_total(response, 2)
        self.assert_values(response, 'name', ['gamma-one', 'delta'])

        response = self._execute_query(value__gt=3)
        self.assert_total(response, 1)
        self.assert_values(response, 'name', ['delta'])

        response = self._execute_query(value__gt=4)
        self.assert_total(response, 0)

    def test_gte_operator(self):
        response = self._execute_query(value__gte=3)
        self.assert_total(response, 2)
        self.assert_values(response, 'name', ['gamma-one', 'delta'])

        response = self._execute_query(value__gte=4)
        self.assert_total(response, 1)
        self.assert_values(response, 'name', ['delta'])

        response = self._execute_query(value__gte=5)
        self.assert_total(response, 0)

    def test_lt_operator(self):
        response = self._execute_query(value__lt=2)
        self.assert_total(response, 2)
        self.assert_values(response, 'name', ['alpha-one', 'alpha-two'])

        response = self._execute_query(value__lt=1)
        self.assert_total(response, 1)
        self.assert_values(response, 'name', ['alpha-one'])

        response = self._execute_query(value__lt=0)
        self.assert_total(response, 0)

    def test_lte_operator(self):
        response = self._execute_query(value__lte=1)
        self.assert_total(response, 2)
        self.assert_values(response, 'name', ['alpha-one', 'alpha-two'])

        response = self._execute_query(value__lte=0)
        self.assert_total(response, 1)
        self.assert_values(response, 'name', ['alpha-one'])

        response = self._execute_query(value__lte=-1)
        self.assert_total(response, 0)

    def test_in_operator(self):
        response = self._execute_query(value__in=[0, 1])
        self.assert_total(response, 2)
        self.assert_values(response, 'name', ['alpha-one', 'alpha-two'])

        response = self._execute_query(value__in=[0, 1, 6, 7])
        self.assert_total(response, 2)
        self.assert_values(response, 'name', ['alpha-one', 'alpha-two'])

        response = self._execute_query(value__in=[6, 7])
        self.assert_total(response, 0)

        response = self._execute_query(name__in=['alpha-one', 'delta'])
        self.assert_total(response, 2)
        self.assert_values(response, 'name', ['alpha-one', 'delta'])

    def test_notin_operator(self):
        response = self._execute_query(value__notin=[0, 1])
        self.assert_total(response, 3)
        self.assert_values(response, 'name', ['beta-one', 'gamma-one', 'delta'])

        response = self._execute_query(value__notin=[0, 1, 6, 7])
        self.assert_total(response, 3)
        self.assert_values(response, 'name', ['beta-one', 'gamma-one', 'delta'])

        response = self._execute_query(value__in=[6, 7])
        self.assert_total(response, 0)

        response = self._execute_query(name__notin=['alpha-one', 'delta'])
        self.assert_total(response, 3)
        self.assert_values(response, 'name', ['alpha-two', 'beta-one', 'gamma-one'])
