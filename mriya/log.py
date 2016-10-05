#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import logging
import sys

def loginit(logging_level):
    logging.basicConfig(level=logging_level,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    logging.getLogger(__name__).setLevel(logging_level)
    logging.getLogger(__name__).info("Log created")
    if logging_level == logging.DEBUG:
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

