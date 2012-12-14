from mesh.constants import *
from mesh.exceptions import GoneError, NotFoundError
from mesh.standard import Controller
from sqlalchemy.sql import asc, desc, func, not_

from spire.core import Unit
from spire.schema import NoResultFound

__all__ = ('ModelController', 'ProxyController',)

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

def parse_attr_mapping(mapping):
    if isinstance(mapping, basestring):
        mapping = mapping.split(' ')
    if isinstance(mapping, (list, tuple)):
        pairs = {}
        for pair in mapping:
            if isinstance(pair, (list, tuple)):
                pairs[pair[0]] = pair[1]
            else:
                pairs[pair] = pair
        mapping = pairs
    return mapping

class ModelController(Unit, Controller):
    """A mesh controller for spire.schema models."""

    model = None
    schema = None
    mapping = None
    polymorphic_mapping = None
    polymorphic_on = None
    operators = FilterOperators()

    @classmethod
    def __construct__(cls):
        Controller.__construct__()
        if cls.resource:
            mapping = cls.polymorphic_mapping
            if mapping:
                for identity, submapping in mapping.items():
                    mapping[identity] = parse_attr_mapping(submapping)
                return

            attr = cls.polymorphic_on
            if attr and not isinstance(attr, tuple):
                cls.polymorphic_on = (attr, attr)

            mapping = cls.mapping
            if mapping is None:
                mapping = cls.resource.filter_schema().keys()
            cls.mapping = parse_attr_mapping(mapping)

    def acquire(self, subject):
        try:
            return self.schema.session.query(self.model).get(subject)
        except NoResultFound:
            return None

    def create(self, request, response, subject, data):
        instance = self.model.polymorphic_create(self._construct_model(data))
        self._annotate_model(request, instance, data)

        self.schema.session.add(instance)
        self.schema.session.commit()
        response({'id': self._get_model_value(instance, 'id')})

    def delete(self, request, response, subject, data):
        subject.session.delete(subject)
        subject.session.commit()
        response({'id': self._get_model_value(subject, 'id')})

    def get(self, request, response, subject, data):
        response(self._construct_resource(request, subject, data))

    def put(self, request, response, subject, data):
        if subject:
            self.update(request, response, subject, data)
        else:
            data['id'] = request.subject
            self.create(request, response, subject, data)

    def query(self, request, response, subject, data):
        data = data or {}
        query = self.schema.session.query(self.model)

        filters = data.get('query')
        if filters:
            query = self._construct_filters(query, filters)

        query = self._annotate_query(request, query, data)

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
            resources.append(self._construct_resource(request, instance, data))

        response({'total': total, 'resources': resources})

    def update(self, request, response, subject, data):
        if data:
            subject.update_with_mapping(self._construct_model(data))
            self._annotate_model(request, subject, data)
            subject.session.commit()
        response({'id': self._get_model_value(subject, 'id')})

    def _annotate_model(self, request, model, data):
        pass

    def _annotate_resource(self, request, model, resource, data):
        pass

    def _annotate_query(self, request, query, data):
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
        mapping = self.mapping
        if self.polymorphic_on:
            mapping = self.polymorphic_mapping[data[self.polymorphic_on[0]]]

        model = {}
        for name, attr in mapping.iteritems():
            if name in data:
                model[attr] = data[name]
        return model

    def _construct_resource(self, request, model, data, **resource):
        mapping = self.mapping
        if self.polymorphic_on:
            identity = getattr(model, self.polymorphic_on[1])
            mapping = self.polymorphic_mapping[identity]

        include = exclude = EMPTY
        if data:
            include = data.get('include', EMPTY)
            exclude = data.get('exclude', EMPTY)

        schema = self.resource.schema
        for name, attr in mapping.iteritems():
            field = schema[name]
            if not field.is_identifier:
                if name in exclude or (field.deferred and name not in include):
                    continue
            try:
                resource[name] = getattr(model, attr)
            except AttributeError:
                pass

        self._annotate_resource(request, model, resource, data)
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


class ProxyController(Unit, Controller):
    """A mesh controller for mesh proxy models."""
    proxy_model = None
    mapping = None

    @classmethod
    def __construct__(cls):
        Controller.__construct__()
        if cls.resource:
            mapping = cls.mapping
            if mapping is None:
                mapping = cls.resource.filter_schema.keys()
            cls.mapping = parse_attr_mapping(mapping)

            cls.id_field = cls.resource.id_field

    def acquire(self, subject):
        try:
            return self.proxy_model.get(subject)
        except GoneError:
            return None

    def create(self, request, response, subject, data):
        proxy_model = self._construct_proxy_model(data)
        self._annotate_proxy_model(request, proxy_model, data)
        subject = self.proxy_model.create(proxy_model)
        id_field = self.id_field
        response({id_field: self._get_proxy_model_value(subject, id_field)})

    def delete(self, request, response, subject, data):
        subject.destroy()
        id_field = self.id_field
        response({id_field: self._get_proxy_model_value(subject, id_field)})

    def get(self, request, response, subject, data):
        resource = self._construct_resource(request, subject, data)
        self._annotate_resource(request, resource, subject, data)
        response(self._prune_resource(resource, data))

    def put(self, request, response, subject, data):
        if subject:
            self.update(request, response, subject, data)
        else:
            data[self.id_field] = request.subject
            self.create(request, response, subject, data)

    def query(self, request, response, subject, data):
        data = data or {}
        if 'query' in data:
            data['query'] = self._construct_filter(data['query'])

        try:
            query_results = self.proxy_model.query(**data).all()
        except NotFoundError:
            query_results = []
            total = 0
            status = OK
        else:
            total = query_results.total
            status = query_results.status

        resources = []
        for result in query_results:
            resource = self._construct_resource(request, result, data)
            self._annotate_resource(request, resource, result, data)
            resources.append(self._prune_resource(resource, data))

        response(status=status, 
                 content={'resources': resources, 'total': total})

    def update(self, request, response, subject, data):
        if data:
            proxy_data = self._construct_proxy_model(data)
            self._annotate_proxy_model(request, proxy_data, data)
            subject.update(proxy_data)
        id_field = self.id_field
        response({id_field: self._get_proxy_model_value(subject, id_field)})

    def _construct_filter(self, filters):
        mapping = self.mapping
        subject_filters = {}
        for filter_operand, value in filters.iteritems():
            filter_operands = filter_operand.rsplit('__', 1)
            filter_operands[0] = mapping[filter_operands[0]]
            subject_filters['__'.join(filter_operands)] = value

        return subject_filters

    def _construct_resource(self, request, subject, data):
        resource = {}
        for res_field, proxy_field in self.mapping.iteritems():
            try:
                resource[res_field] = getattr(subject, proxy_field)
            except AttributeError:
                continue
        return resource

    def _construct_proxy_model(self, data):
        subject = {}
        mapping = self.mapping
        for field, value in data.iteritems():
            subject_field = mapping[field]
            subject[subject_field] = value
        return subject

    def _get_proxy_model_value(self, subject, field):
        model_field = self.mapping[field]
        return getattr(subject, model_field)

    def _annotate_resource(self, request, resource, proxy_model, data):
        pass

    def _annotate_proxy_model(self, request, proxy_data, data):
        pass
