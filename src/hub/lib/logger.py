# -*- coding: utf-8 -*-
'''
    hub.log
    ~~~~~~~~

    This is where Hub's logging gets set up.
'''

# Import core python libs
import os
import sys
import logging
import logging.handlers

LOG_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARN,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}

__CONSOLE_CONFIGURED = False
__LOGFILE_CONFIGURED = False


def is_console_configured():
    return __CONSOLE_CONFIGURED


def is_logfile_configured():
    return __LOGFILE_CONFIGURED


def is_logging_configured():
    if not __CONSOLE_CONFIGURED and not __LOGFILE_CONFIGURED:
        return False
    return True


def setup():
    if is_logging_configed():
        logger = logging.getLogger(__name__)
        return logger


def log_to_console(name=None, level='warning', format=None, date=None,
                   trace=False):
    if is_console_configured():
        logger = logging.getLogger(__name__)
        logger.warn('Console logging already configured')
        return logger

    if not name:
        name = 'hub'
    else:
        name = 'hub.' + name

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    handler = None
    for handler in logging.root.handlers:
        if handler.stream is sys.stderr:
            # There's already a logging handler outputting to sys.stderr
            break
    else:
        handler = logging.StreamHandler()

    if not format:
        format = "%(message)s"
    if trace:  # Ignores custom formats
        format = "%(asctime)s %(name)-16s %(levelname)-5s %(message)s \
{%(pathname)s:%(lineno)d:%(funcName)s}"
    formatter = logging.Formatter(format, datefmt=date)
    handler.setFormatter(formatter)

    log_level = LOG_LEVELS.get(level.lower(), logging.WARN)
    if trace:
        log_level = logging.DEBUG
    handler.setLevel(log_level)

    logger.addHandler(handler)

    global __CONSOLE_CONFIGURED
    __CONSOLE_CONFIGURED = True

    return logger


def log_to_file(name=None, level='warning', format=None, date=None,
                log_file=None, max_size=None, retain=None, trace=False):

    if is_logfile_configured():
        logger = logging.getLogger(__name__)
        logger.warn('Logfile logging already configured')
        return logger

    if not name:
        name = 'hub'
    else:
        name = 'hub.' + name

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not log_file:
        log_file = '/tmp/hub.log'
    if not max_size:
        max_size = 1024*1024*5  # 5MBytes
    if not retain:
        retain = 5

    try:
        handler = logging.handlers.RotatingFileHandler(log_file,
                                                       maxBytes=max_size,
                                                       backupCount=retain)
    except (IOError, OSError):
        sys.stderr.write(
            'Failed to open log file, do you have permission to write to '
            '{0}\n'.format(log_file))
        sys.exit(2)

    if not format:
        format = "%(asctime)s %(name)-16s %(levelname)-5s %(message)s"
    if trace:
        format = format + ' {%(pathname)s:%(lineno)d:%(funcName)s}'
    formatter = logging.Formatter(format, datefmt=date)
    handler.setFormatter(formatter)

    log_level = LOG_LEVELS.get(level.lower(), logging.WARN)
    if trace:
        log_level = logging.DEBUG
    handler.setLevel(log_level)

    logger.addHandler(handler)

    global __LOGFILE_CONFIGURED
    __LOGFILE_CONFIGURED = True

    return logger

if __name__ == '__main__':
    pass
