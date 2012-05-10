from mesh.standard import Controller
from sqlalchemy.sql import asc, desc, func, not_

from spire.core import Unit

__all__ = ('ModelController',)

EMPTY = []

class FilterOperators(object):
    def equal_op(self, query, column, value):
        return query.filter(column == value)

    def iequal_op(self, query, column, value):
        return query.filter(func.lower(column) == value)

    def not_op(self, query, column, value):
        return query.filter(column != value)

    def inot_op(self, query, column, value):
        return query.filter(func.lower(column) != value)

    def prefix_op(self, query, column, value):
        return query.filter(column.like(value + '%'))

    def iprefix_op(self, query, column, value):
        return query.filter(column.ilike(value + '%'))

    def suffix_op(self, query, column, value):
        return query.filter(column.like('%' + value))

    def isuffix_op(self, query, column, value):
        return query.filter(column.ilike('%' + value))

    def contains_op(self, query, column, value):
        return query.filter(column.like('%' + value + '%'))

    def icontains_op(self, query, column, value):
        return query.filter(column.ilike('%' + value + '%'))

    def gt_op(self, query, column, value):
        return query.filter(column > value)

    def gte_op(self, query, column, value):
        return query.filter(column >= value)

    def lt_op(self, query, column, value):
        return query.filter(column < value)

    def lte_op(self, query, column, value):
        return query.filter(column <= value)

    def null_op(self, query, column, value):
        if value:
            return query.filter(column == None)
        else:
            return query.filter(column != None)

    def in_op(self, query, column, value):
        return query.filter(column.in_(value))

    def notin_op(self, query, column, value):
        return query.filter(not_(column.in_(value)))

class ModelController(Unit, Controller):
    """A mesh controller for spire.schema models."""

    model = None
    schema = None
    mapping = None
    operators = FilterOperators()

    @classmethod
    def __construct__(cls):
        Controller.__construct__()
        if cls.resource:
            mapping = cls.mapping
            if mapping is None:
                mapping = cls.resource.filter_schema().keys()
            if isinstance(mapping, basestring):
                mapping = mapping.split(' ')
            if isinstance(mapping, (list, tuple)):
                mapping = dict(zip(mapping, mapping))
            cls.mapping = mapping

    def acquire(self, subject):
        try:
            return self.schema.session.query(self.model).get(subject)
        except ValueError:
            return None

    def create(self, context, response, subject, data):
        instance = self.model(**self._construct_model(data))
        self.schema.session.add(instance)
        self.schema.session.commit()
        response({'id': self._get_model_value(instance, 'id')})

    def delete(self, context, response, subject, data):
        subject.session.delete(subject)
        subject.session.commit()
        response({'id': self._get_model_value(subject, 'id')})

    def get(self, context, response, subject, data):
        response(self._construct_resource(subject, data))

    def put(self, context, response, subject, data):
        if subject:
            self.update(context, response, subject, data)
        else:
            self.create(context, response, subject, data)

    def query(self, context, response, subject, data):
        data = data or {}
        query = self.schema.session.query(self.model)

        filters = data.get('query')
        if filters:
            query = self._construct_filters(query, filters)

        query = self._annotate_query(query, data)

        total = query.count()
        if data.get('total'):
            return response({'total': total})

        if 'sort' in data:
            query = self._construct_sorting(query, data['sort'])
        if 'limit' in data:
            query = query.limit(data['limit'])
        if 'offset' in data:
            query = query.offset(data['offset'])

        resources = []
        for instance in query.all():
            resources.append(self._construct_resource(instance, data))

        response({'total': total, 'resources': resources})

    def update(self, context, response, subject, data):
        subject.update_with_mapping(self._construct_model(data))
        subject.session.commit()
        response({'id': self._get_model_value(subject, 'id')})

    def _annotate_model(self, model, data):
        pass

    def _annotate_resource(self, model, resource, data):
        pass

    def _annotate_query(self, query, data):
        return query

    def _construct_filters(self, query, filters):
        model, operators = self.model, self.operators
        for filter, value in filters.iteritems():
            attr, operator = filter, 'equal'
            if '__' in filter:
                attr, operator = filter.rsplit('__', 1)

            column = getattr(model, self.mapping[attr])
            if not column:
                # TODO
                continue

            constructor = getattr(operators, operator + '_op')
            query = constructor(query, column, value)

        return query

    def _construct_model(self, data):
        model = {}
        for name, attr in self.mapping.iteritems():
            if name in data:
                model[attr] = data[name]

        self._annotate_model(model, data)
        return model

    def _construct_resource(self, model, data, **resource):
        include = exclude = EMPTY
        if data:
            include = data.get('include', EMPTY)
            exclude = data.get('exclude', EMPTY)

        schema = self.resource.schema
        for name, attr in self.mapping.iteritems():
            field = schema[name]
            if not field.is_identifier:
                if name in exclude or (field.deferred and name not in include):
                    continue
            try:
                resource[name] = getattr(model, attr)
            except AttributeError:
                pass

        self._annotate_resource(model, resource, data)
        return resource

    def _construct_sorting(self, query, sorting):
        columns = []
        for attr in sorting:
            direction = asc
            if attr[-1] == '+':
                attr = attr[:-1]
            elif attr[-1] == '-':
                attr = attr[:-1]
                direction = desc

            column = getattr(self.model, self.mapping[attr])
            if not column:
                continue

            columns.append(direction(column))

        return query.order_by(*columns)

    def _get_model_value(self, model, name):
        return getattr(model, self.mapping[name])
