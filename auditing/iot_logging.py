import logging.config, os
from logging.handlers import RotatingFileHandler

level = logging.DEBUG if os.environ.get("ENVIRONMENT") == "DEV" else logging.INFO
os.makedirs("logs", exist_ok=True)
LOG_CONFIG = {'version': 1,
              'formatters': {
                  'detailed': {
                      'class': 'logging.Formatter',
                      'format': '%(asctime)s %(name)s %(levelname)s %(message)s'
                  },
                  'simple': {
                      'class': 'logging.Formatter',
                      'format': '%(message)s'
                  }
              },
              'handlers': {
                  'console': {
                      'class': 'logging.StreamHandler',
                      'formatter': 'detailed',
                      'level': level
                  },
                  'file': {
                      'class': 'logging.handlers.RotatingFileHandler',
                      'filename': 'logs/orchestrator.log',
                      'mode': 'a',
                      'maxBytes': 10485760,  # 10MB
                      'backupCount': 3,
                      'formatter': 'detailed',
                      'level': level}},
              'root': {'handlers': ('console', 'file')}}
logging.config.dictConfig(LOG_CONFIG)


def getLogger(name):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger
