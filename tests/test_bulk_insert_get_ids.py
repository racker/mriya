#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import logging
import sys
from logging import getLogger
from configparser import ConfigParser
from data_connector import RESTConnector
from data_connector import get_conn_param
from data_connector import conn_param_set_token
from data_connector import AuthToken
from test_data_generator import DataGenerator
from test_data_generator import data_struct
from test_data_generator import defaults
from test_data_generator import prefixes
from sf_bulk_connector import SfBulkConnector
from bulk_data import csv_from_list_of_dicts

config_file = 'test-config.ini'

def setup():
    logging.basicConfig(level=logging.DEBUG,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    getLogger(__name__).setLevel(logging.DEBUG)
    getLogger(__name__).info("Log created")
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


def test_insert():
    setup()
    config = ConfigParser()
    with open(config_file, 'r') as conf_file:
        config.read_file(conf_file)
    conn_param = get_conn_param(config['test'])
    print "ok"
    auth_token = AuthToken(conn_param, 'sessions.ini')
    conn_param = auth_token.conn_param_with_token()
    conn = SfBulkConnector(conn_param)

    #datagen = DataGenerator(data_struct, defaults, prefixes)
    #new_data = datagen.gen_data(12, 'Account')
    #csv_data = csv_from_list_of_dicts(new_data)
    #print csv_data
    with open('test.csv') as inp:
        csv_data = inp.read()
    result_ids = conn.bulk_insert('Account', csv_data)
    print result_ids

if __name__ == '__main__':
    test_insert()
    
