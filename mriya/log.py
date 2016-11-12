#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import logging
import sys

LOGGING_LEVEL = logging.INFO
STDOUT = 'stdout' #reserved name
STDERR = 'error.log' #reserved name
LOG = 'mriya.log'  #reserved name

def loginit(name):
    if name == STDERR:
        file_handler = logging.FileHandler(STDERR, 'w')
        log_format = '%(asctime)s %(levelname)-8s %(message)s'
    elif name == LOG:
        file_handler = logging.FileHandler(LOG, 'w')
        log_format = '%(asctime)s %(levelname)-8s %(message)s'
    elif name == STDOUT:
        file_handler = logging.StreamHandler(sys.stdout)
        log_format = '%(message)s'
    if not file_handler:
        return
    file_handler.setFormatter(logging.Formatter(log_format))
    logger = logging.getLogger(name)
    logger.setLevel(LOGGING_LEVEL)
    logger.addHandler(file_handler)
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

