from unittest2 import TestCase

from scheme import *

from spire.core import *

class TestUnits(TestCase):
    def setUp(self):
        Registry.purge()

    def _test_schema_structure(self, structure, *tests):
        if isinstance(structure, Structure):
            structure = structure.structure
        
        for name, field in tests:
            self.assertIn(name, structure)
            self.assertIsInstance(structure[name], field)

    def test_simple_declaration(self):
        class TestUnit(Unit):
            pass

        self.assertEqual(TestUnit.identity, 'tests.test_unit.TestUnit')
        self.assertIsNone(TestUnit.configuration)
        self.assertEqual(TestUnit.dependencies, {})
        self.assertIs(TestUnit, Registry.units[TestUnit.identity])

    def test_configuration_declaration(self):
        class TestUnit(Unit):
            configuration = Configuration({
                'param': Text(),
            })

        self.assertIsInstance(TestUnit.configuration, Configuration)
        self.assertIs(TestUnit.configuration.subject, TestUnit)
        self.assertIsInstance(TestUnit.configuration.schema.structure['param'], Text)

    def test_configuration_inheritance(self):
        class FirstUnit(Unit):
            configuration = Configuration({
                'first': Text(),
            })

        class SecondUnit(Unit):
            configuration = Configuration({
                'second': Integer(),
            })

        class ThirdUnit(FirstUnit, SecondUnit):
            pass

        self.assertIsInstance(ThirdUnit.configuration, Configuration)
        self.assertIs(ThirdUnit.configuration.subject, ThirdUnit)
        self._test_schema_structure(ThirdUnit.configuration.schema,
            ('first', Text), ('second', Integer))
        
        class FourthUnit(FirstUnit, SecondUnit):
            configuration = Configuration({
                'fourth': Boolean(),
            })
    
        self.assertIsInstance(FourthUnit.configuration, Configuration)
        self.assertIs(FourthUnit.configuration.subject, FourthUnit)
        self._test_schema_structure(FourthUnit.configuration.schema,
            ('first', Text), ('second', Integer), ('fourth', Boolean))

        class FifthUnit(FirstUnit, SecondUnit):
            configuration = Configuration({
                'first': DateTime(),
                'fifth': Date(),
            })

        self.assertIsInstance(FifthUnit.configuration, Configuration)
        self.assertIs(FifthUnit.configuration.subject, FifthUnit)
        self._test_schema_structure(FifthUnit.configuration.schema,
            ('first', DateTime), ('second', Integer), ('fifth', Date))

    def test_dependency_declaration(self):
        class FirstUnit(Unit):
            pass

        dependency = Dependency(FirstUnit)
        self.assertIs(dependency.attr, None)
        self.assertIs(dependency.dependent, None)
        self.assertIs(dependency.unit, FirstUnit)

        class SecondUnit(Unit):
            first = dependency

        self.assertIsInstance(SecondUnit.dependencies, dict)
        self.assertIs(SecondUnit.dependencies['first'], dependency)
        self.assertEqual(dependency.attr, 'first')
        self.assertIs(dependency.dependent, SecondUnit)

    def test_dependency_inheritance(self):
        def check(unit, *tests):
            dependencies = unit.dependencies
            for attr, dependency in tests:
                self.assertIn(attr, dependencies)
                self.assertIsInstance(dependencies[attr], Dependency)
                self.assertEqual(dependencies[attr].attr, attr)
                self.assertIs(dependencies[attr].dependent, unit)
                self.assertIs(dependencies[attr].unit, dependency.unit)
                self.assertIsNot(dependencies[attr], dependency)
                self.assertIs(getattr(unit, attr), dependencies[attr])

        class FirstUnit(Unit):
            pass

        class SecondUnit(Unit):
            pass

        first_dependency = Dependency(FirstUnit)
        class ThirdUnit(Unit):
            first = first_dependency

        second_dependency = Dependency(SecondUnit)
        class FourthUnit(Unit):
            second = second_dependency

        class FifthUnit(ThirdUnit, FourthUnit):
            pass

        self.assertIsInstance(FifthUnit.dependencies, dict)
        check(FifthUnit, ('first', first_dependency), ('second', second_dependency))

        third_dependency = Dependency(ThirdUnit)
        class SixthUnit(ThirdUnit, FourthUnit):
            third = third_dependency

        self.assertIsInstance(SixthUnit.dependencies, dict)
        check(SixthUnit, ('first', first_dependency), ('second', second_dependency))
        self.assertIs(SixthUnit.dependencies['third'], third_dependency)
        self.assertEqual(third_dependency.attr, 'third')
        self.assertIs(third_dependency.dependent, SixthUnit)

    def test_simple_instantiation(self):
        class TestUnit(Unit):
            def __init__(self):
                self.sentinel = True

        unit = TestUnit()
        self.assertEqual(unit.__identity__, TestUnit.identity)
        self.assertEqual(unit.__token__, TestUnit.identity)
        self.assertEqual(unit.configuration, None)
        self.assertTrue(unit.sentinel)
        self.assertIs(unit.__assembly__, Assembly.standard)

    def test_configured_instantiation(self):
        class TestUnit(Unit):
            configuration = Configuration({
                'text': Text(),
                'bool': Boolean(),
                'default': Integer(default=1),
            })

            def __init__(self, text=None):
                self.text = text

        unit = TestUnit()
        self.assertEqual(unit.configuration, {'default': 1})
        self.assertIsNone(unit.text)

        unit = TestUnit(text='stuff')
        self.assertEqual(unit.configuration, {'text': 'stuff', 'default': 1})
        self.assertEqual(unit.text, 'stuff')

        unit = TestUnit(text='stuff', default=2)
        self.assertEqual(unit.configuration, {'text': 'stuff', 'default': 2})
        self.assertEqual(unit.text, 'stuff')

    def test_instantiation_with_alternate_assembly(self):
        class TestUnit(Unit):
            pass

        assembly = Assembly()
        with assembly:
            unit = TestUnit()

        self.assertIs(unit.__assembly__, assembly)
        self.assertIsNot(unit.__assembly__, Assembly.standard)
