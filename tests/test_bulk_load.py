#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import logging
import sys
import pprint
from logging import getLogger
from configparser import ConfigParser
from mriya.data_connector import RESTConnector
from mriya.data_connector import get_conn_param
from mriya.data_connector import conn_param_set_token
from mriya.data_connector import AuthToken
from mriya.test_data_generator import DataGenerator
from mriya.test_data_generator import data_struct
from mriya.test_data_generator import defaults
from mriya.test_data_generator import prefixes
from mriya.sf_bulk_connector import SfBulkConnector
from mriya.bulk_data import csv_from_list_of_dicts
from mriya.bulk_data import parse_batch_res_data

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

    # WHERE Id = '001n0000009bI3MAAU'
    csv_rows = conn.bulk_load('Account', "SELECT Account_Birthday__c,Name,Id FROM Account WHERE Id = '001n0000009bI3MAAU'")
    try:
        csv_rows2 = conn.bulk_load('Account', "SELECT Account_Birthday__c,Name,Id,BillingAddress FROM Account WHERE Id = '001n0000009bI3MAAU'")
        assert 0
    except:
        pass

if __name__ == '__main__':
    test_insert()
    
