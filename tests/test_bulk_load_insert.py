#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import logging
import sys
import pprint
from StringIO import StringIO
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
from mriya.bulk_data import get_bulk_data_from_csv_stream
from mriya.bulk_data import csv_from_bulk_data
from mriya.bulk_data import BulkData
from mriya.log import loginit

SF_NULL_VALUE = '#N/A'
config_file = 'test-config.ini'

TEST_CSV_INSERT = 'Name,Account_Birthday__c,Billing_Address__c,type\n\
mriya,#N/A,Street_Billing_Address_CO9S63EMH4,Account\n\
mriya,"2015-05-15","Street_Billing_Address ""PICGZSLC0F""",Account\n\
mriya,"2005-08-13","Street_Billing_Address\n\
2YNSCQEHFX",Account\n\
mriya,"2000-10-23","Street_Billing_Address, 7VLWJ7CMQX",Account\n'

TEST_CSV_UPSERT = 'Name,Account_Birthday__c,Billing_Address__c,type\n\
mriya,#N/A,Street_Billing_Address_CO9S63EMH4,Account\n'

def columns_compare(col1, col2):
    for idx in xrange(len(col1)):
        if col1[idx] == SF_NULL_VALUE:
            if col2[idx] != '' and col2[idx] != SF_NULL_VALUE:
                return False
        elif col2[idx] == SF_NULL_VALUE:
            if col1[idx] != '' and col1[idx] != SF_NULL_VALUE:
                return False
        elif col1[idx] != col2[idx]:
            return False
    return True

def assert_equality(bulk_data1, bulk_data2):
    fields1 = [x.lower() for x in bulk_data1.fields]
    fields2 = [x.lower() for x in bulk_data2.fields]
    assert fields1 == fields2
    assert len(bulk_data1.rows) == len(bulk_data2.rows)
    for field_idx in xrange(len(bulk_data1.fields)):
        column1 = []
        column2 = []
        for row_idx in xrange(len(bulk_data1.rows)):
            column1.append( bulk_data1.rows[row_idx][field_idx] )
            column2.append( bulk_data2.rows[row_idx][field_idx] )
        column1.sort()
        column2.sort()
        print column1
        print column2
        assert columns_compare(column1, column2)

def fetch_records_by_returned_ids(conn, result_ids, columns):
    id_idx = result_ids.fields.index('Id')
    success_idx = result_ids.fields.index('Success')

    where = ' OR '.join(["Id='%s'"%x[id_idx] for x in result_ids.rows if x[success_idx] == 'true'])

    query = "SELECT %s FROM Account WHERE %s" % (columns, where)
    print query
    csv_rows = conn.bulk_load('Account', query)
    selected = parse_batch_res_data(csv_rows)
    return selected

def test_insert_load():
    loginit(__name__)
    config = ConfigParser()
    with open(config_file, 'r') as conf_file:
        config.read_file(conf_file)
    conn_param = get_conn_param(config['test'])
    print "ok"
    auth_token = AuthToken(conn_param, 'sessions.ini')
    conn_param = auth_token.conn_param_with_token()
    conn = SfBulkConnector(conn_param)

    ####### INSERT #####
    csv_data = TEST_CSV_INSERT
    bulk_result_ids = conn.bulk_insert('Account', csv_data, 1000, False)
    # Retrieve ids of inserted results
    result_ids = parse_batch_res_data(bulk_result_ids)

    ###### SELECT #####
    selected = fetch_records_by_returned_ids(
        conn, result_ids, "Name,Account_Birthday__c,Billing_Address__c,type")
    # check if inserted data is equal to retrieved

    expected = get_bulk_data_from_csv_stream(StringIO(TEST_CSV_INSERT))
    try:
        assert_equality(expected, selected)
    except:
        print "selected", selected
        print "expected", expected
        raise

def test_insert_update():
    loginit(__name__)
    config = ConfigParser()
    with open(config_file, 'r') as conf_file:
        config.read_file(conf_file)
    conn_param = get_conn_param(config['test'])
    auth_token = AuthToken(conn_param, 'sessions.ini')
    conn_param = auth_token.conn_param_with_token()
    conn = SfBulkConnector(conn_param)

    ####### INSERT #####
    csv_data = TEST_CSV_UPSERT
    bulk_result_ids = conn.bulk_insert('Account', csv_data, 1000, False)
    # Retrieve ids of inserted results
    result_ids = parse_batch_res_data(bulk_result_ids)
    print "ok insert"

    ###### UPDATE #####
    selected_with_ids = fetch_records_by_returned_ids(
        conn, result_ids, "Id,Name,Account_Birthday__c,Billing_Address__c,type")
    # check if inserted data is equal to retrieved

    name_idx = selected_with_ids.fields.index('Name')
    rows = []
    for item in selected_with_ids.rows:
        item_update = list(item)
        item_update[name_idx] = 'BLA-BLA-BLA'
        rows.append(tuple(item_update))
    update_data = BulkData(selected_with_ids.fields, rows)

    csv_update = csv_from_bulk_data(update_data)
    print "csv_update", csv_update
    update_bulk_result_ids = conn.bulk_update('Account', csv_update, 1000, False)
    # Retrieve ids of inserted results
    update_result_ids = parse_batch_res_data(update_bulk_result_ids)
    print "update_result_ids", update_result_ids
    ###### SELECT #####
    selected_update = fetch_records_by_returned_ids(
        conn, update_result_ids,
        "Id,Name,Account_Birthday__c,Billing_Address__c,type")
    # check if inserted data is equal to retrieved
    try:
        assert_equality(selected_update, update_data)
    except:
        print "update_data", update_data
        print "selected_update", selected_update
        raise


if __name__ == '__main__':
    test_insert_update()
    test_insert_load()
    
