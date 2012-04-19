import jinja2
from jinja2 import ChoiceLoader, PackageLoader
from jinja2.filters import urlize

STANDARD_EXTENSIONS = ['jinja2.ext.loopcontrols', 'jinja2.ext.with_']

class TemplateEnvironment(jinja2.Environment):
    def __init__(self, paths, extensions=None):
        loaders = []
        for path in paths:
            loaders.append(PackageLoader(*path))

        if len(loaders) == 1:
            loader = loaders[0]
        else:
            loader = ChoiceLoader(loaders)

        extensions = set(extensions or [])
        extensions.update(STANDARD_EXTENSIONS)

        super(TemplateEnvironment, self).__init__(loader=loader, extensions=extensions)

    def render_template(self, template, context=None):
        return self.get_template(template).render(context or {})
