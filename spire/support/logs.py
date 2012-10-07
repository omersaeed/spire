import logging
from datetime import datetime

try:
    from logging.config import dictConfig
except ImportError:
    dictConfig = None

def configure_logging(configuration):
    if 'version' not in configuration:
        configuration['version'] = 1
    if dictConfig:
        dictConfig(configuration)

class LogFormatter(logging.Formatter):
    def __init__(self, format='%(timestamp)s %(name)s %(levelname)s %(message)s'):
        logging.Formatter.__init__(self, format)

    def format(self, record):
        record.timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        return logging.Formatter.format(self, record)

class LogHelper(object):
    LEVELS = { 
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL,
    }   

    def __init__(self, logger):
        if isinstance(logger, basestring):
            logger = logging.getLogger(logger)
        self.logger = logger

    def __call__(self, level, message, *args):
        if level == 'exception':
            self.logger.exception(message, *args)
        else:
            self.logger.log(self.LEVELS[level], message, *args)

    def enabled(self, level):
        return self.logger.isEnabledFor(self.LEVELS[level])
