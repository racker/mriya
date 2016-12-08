#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import logging
import sys

LOGGING_LEVEL = logging.INFO
STDOUT = 'stdout' #reserved name
STDERR = 'stderr' #reserved name
LOG = 'mriya.log' #reserved name

# to avoid add loggers having the same names
INITIALIZED_LOGGERS = []

def defaultlog():
    loginit(STDOUT)
    loginit(LOG, STDOUT)
    loginit(STDERR, STDOUT)

def loginit(name, log_to=None):
    if name in INITIALIZED_LOGGERS:
        return

    if not log_to:
        log_to = name

    if name == LOG:
        file_handler = logging.FileHandler(log_to, 'w')
        log_format = '%(asctime)s %(levelname)-8s %(message)s'
    elif log_to == STDOUT:
        file_handler = logging.StreamHandler(sys.stdout)
        log_format = '%(message)s'
    elif log_to == STDERR:
        file_handler = logging.StreamHandler(sys.stderr)
        log_format = '%(message)s'
    else:
        loginit(name, log_to=STDOUT)
        defaultlog()
        return

    if not file_handler:
        return
    file_handler.setFormatter(logging.Formatter(log_format))
    logger = logging.getLogger(name)
    logger.setLevel(LOGGING_LEVEL)
    logger.addHandler(file_handler)
    INITIALIZED_LOGGERS.append(name)
    if LOGGING_LEVEL == logging.DEBUG:
        # These two lines enable debugging at httplib level
        # (requests->urllib3->http.client) You will see the REQUEST,
        # including HEADERS and DATA, and RESPONSE with HEADERS but
        # without DATA.  The only thing missing will be the response.body
        # which is not logged.
        try:
            import http.client as http_client
        except ImportError:
            # Python 2
            import httplib as http_client
        http_client.HTTPConnection.debuglevel = 1

