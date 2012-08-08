class SpireError(Exception):
    """..."""

class ConfigurationError(SpireError):
    """..."""

class LocalError(SpireError):
    """..."""

    @classmethod
    def construct(cls, name):
        return cls('a value for %r is not available in the local context' % name)

class TemporaryStartupError(SpireError):
    """..."""
