from mesh.standard import Controller
from spire.core import Unit

__all__ = ('ModelController',)

class ModelController(Unit, Controller):
    """A mesh controller for spire.schema models."""

    model = None
    schema = None
    mapping = None

    def __init__(self):
        if self.mapping is None:
            fields = self.resource.filter_schema().keys()
            self.mapping = dict(zip(fields, fields))

    def acquire(self, subject):
        try:
            return self.schema.session.query(self.model).get(subject)
        except ValueError:
            return None

    def create(self, context, response, subject, data):
        instance = self.model(**self._construct_model(data))
        self.schema.session.add(instance)
        self.schema.session.commit()
        response({'id': instance.id})

    def delete(self, context, response, subject, data):
        subject.session.delete(subject)
        subject.session.commit()
        response({'id': subject.id})

    def get(self, context, response, subject, data):
        response(self._construct_resource(subject))

    def put(self, context, response, subject, data):
        model = self._construct_model(data)
        if subject:
            subject.update_with_mapping(model)
        else:
            subject = self.model(**model)
            self.schema.session.add(subject)

        self.schema.session.commit()
        response({'id': subject.id})

    def query(self, context, response, subject, data):
        data = data or {}
        query = self.schema.session.query(self.model)


        total = query.count()

        if 'limit' in data:
            query = query.limit(data['limit'])
        if 'offset' in data:
            query = query.offset(data['offset'])


        resources = []
        for instance in query.all():
            print instance.fullname, instance.tenant_id
            resources.append(self._construct_resource(instance))

        response({'total': total, 'resources': resources})

    def update(self, context, response, subject, data):
        subject.update_with_mapping(self._construct_model(data))
        subject.session.commit()
        response({'id': subject.id})

    def _construct_model(self, data):
        model = {}
        for name, attr in self.mapping.iteritems():
            if name in data:
                model[attr] = data[name]
        return model

    def _construct_resource(self, model, **resource):
        for name, attr in self.mapping.iteritems():
            try:
                resource[name] = getattr(model, attr)
            except AttributeError:
                pass
        return resource
