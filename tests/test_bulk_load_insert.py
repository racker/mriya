"""
Copyright (C) 2016-2017 by Yaroslav Litvinov <yaroslav.litvinov@gmail.com>
and associates (see AUTHORS).

This file is part of Mriya.

Mriya is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Mriya is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Mriya.  If not, see <http://www.gnu.org/licenses/>.
"""

#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"

import mock
import requests_mock
import mockers #local
import sfbulk

import tempfile
import logging
import sys
import pprint
from os import remove
from StringIO import StringIO
from logging import getLogger
from configparser import ConfigParser
from mriya.config import DEFAULT_SETTINGS_SECTION, SESSIONS_SETTING
from mriya.data_connector import create_bulk_connector
from mriya.sf_bulk_connector import SfBulkConnector
from mriya.bulk_data import parse_batch_res_data
from mriya.bulk_data import get_bulk_data_from_csv_stream
from mriya.bulk_data import csv_from_bulk_data
from mriya.bulk_data import BulkData
from mriya.log import loginit
from mriya import sf_bulk_connector
from mockers import mock_oauth, mock_login
from mriya_dmt import run_job_from_file    
from mriya.sql_executor import setdatadir

SF_NULL_VALUE = '#N/A'
config_file = 'test-config.ini'

TEST_CSV_INSERT = 'Name,Account_Birthday__c,Billing_Address__c,type\n\
mriya,#N/A,Street_Billing_Address_CO9S63EMH4,Account\n\
mriya,"2015-05-15","Street_Billing_Address ""PICGZSLC0F""",Account\n\
mriya,"2005-08-13","Street_Billing_Address\n\
2YNSCQEHFX",Account\n\
mriya,"2000-10-23","Street_Billing_Address, 7VLWJ7CMQX",Account\n'

TEST_CSV_INSERT2 = 'Name,Account_Birthday__c,Billing_Address__c,type\n\
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

def setup():
    loginit(__name__)
    config = ConfigParser()
    with open(config_file, 'r') as conf_file:
        config.read_file(conf_file)
    # test case when sessions file exists but has no token
    sessions_file_name = config[DEFAULT_SETTINGS_SECTION][SESSIONS_SETTING]
    with open(sessions_file_name, 'w') as sessions_f:
        sessions_f.write('{"someuser": "someaccesstoken"}')
    return config

@mock.patch.object(sfbulk.callout.Callout, 'docall')
@requests_mock.Mocker()
def test_insert_load(mock_docall, m):
    # mock setup
    sf_bulk_connector.JOB_CHECK_TIMER = 0    
    mockers.mock_insert_load_delete(mock_docall, m)
    conn = create_bulk_connector(setup(), 'test')

    ####### INSERT #####
    csv_data = TEST_CSV_INSERT
    bulk_result_ids = conn.bulk_insert('Account', csv_data, 1000, False)
    # Retrieve ids of inserted results
    result_ids = parse_batch_res_data(bulk_result_ids)
    print "result_ids", result_ids
    
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
    
    ###### DELETE #####
    delid = result_ids.rows[0][0]
    csv_delete_data = '"Id"\n"%s"' % (delid)
    del_result_ids = conn.bulk_delete('Account', csv_delete_data, 100, False)
    # Retrieve ids of inserted results
    del_ids = parse_batch_res_data(del_result_ids)
    expected_del_res = BulkData(fields=['Id', 'Success', 'Created', 'Error'],
                            rows=[(delid, u'true', u'false', u'')])
    assert_equality(expected_del_res, del_ids)

@mock.patch.object(sfbulk.callout.Callout, 'docall')
@requests_mock.Mocker()
def test_insert_update(mock_docall, m):
    # mock setup    
    sf_bulk_connector.JOB_CHECK_TIMER = 0
    mockers.mock_insert_update(mock_docall, m)

    config = setup()
    # test case when sessions file doesn't exist
    sessions_file_name = config[DEFAULT_SETTINGS_SECTION][SESSIONS_SETTING]
    remove(sessions_file_name)
    
    conn = create_bulk_connector(config, 'test')

    ####### INSERT #####
    csv_data = TEST_CSV_INSERT2
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


@requests_mock.Mocker()
def test_upsert_unsupported(m):
    mock_oauth(m)
    mock_login(m)
    setdatadir(tempfile.mkdtemp())
    with open(config_file) as conf_file:
        with open('tests/sql/upsert_unsupported.sql') as job_file:
            try:
                run_job_from_file(conf_file, job_file,
                                  {'src':'test', 'dst':'test'}, {}, None, None)
                # it should fail
                assert(0)
            except SystemExit:
                pass

@requests_mock.Mocker()
def test_merge_morethan_three_error(m):
    mock_oauth(m)
    mock_login(m)
    setdatadir(tempfile.mkdtemp())
    with open(config_file) as conf_file:
        with open('tests/sql/merge_more_than_three_error.sql') as job_file:
            try:
                run_job_from_file(conf_file, job_file,
                                  {'src':'test', 'dst':'test'}, {}, None, None)
                # it should fail
                assert(0)
            except SystemExit:
                pass

@requests_mock.Mocker()
def test_merge_bad_ids_error(m):
    mock_oauth(m)
    mock_login(m)
    setdatadir(tempfile.mkdtemp())
    with open(config_file) as conf_file:
        with open('tests/sql/merge_bad_ids_errorr.sql') as job_file:
            try:
                run_job_from_file(conf_file, job_file,
                                  {'src':'test', 'dst':'test'}, {}, None, None)
                # it should fail
                assert(0)
            except SystemExit:
                pass 
            
@requests_mock.Mocker()
def test_merge_required_columns_error(m):
    mock_oauth(m)
    mock_login(m)
    setdatadir(tempfile.mkdtemp())
    with open(config_file) as conf_file:
        with open('tests/sql/merge_required_columns_error.sql') as job_file:
            try:
                run_job_from_file(conf_file, job_file,
                                  {'src':'test', 'dst':'test'}, {}, None, None)
                # it should fail
                assert(0)
            except SystemExit:
                pass 
            
@requests_mock.Mocker()
def test_merge_bad_ascii_error(m):
    mock_oauth(m)
    mock_login(m)
    setdatadir(tempfile.mkdtemp())
    with open(config_file) as conf_file:
        with open('tests/sql/merge_bad_ascii.sql') as job_file:
            try:
                run_job_from_file(conf_file, job_file,
                                  {'src':'test', 'dst':'test'}, {}, None, None)
                # it should fail
                assert(0)
            except UnicodeEncodeError:
                pass 
            
            
@requests_mock.Mocker()
def test_delete_syntax(m):
    # this delete operation should fail anyway but improves coverage
    mock_oauth(m)
    mock_login(m)
    setdatadir(tempfile.mkdtemp())
    with open(config_file) as conf_file:
        with open('tests/sql/delete_fake.sql') as job_file:
            try:
                run_job_from_file(conf_file, job_file,
                                  {'src':'test', 'dst':'test'}, {}, None, None)
                # it should fail
                assert(0)
            except:
                pass

@mock.patch.object(sfbulk.callout.Callout, 'docall')
@requests_mock.Mocker()
def test_empty_query_res(mock_docall, m):
    # mock setup    
    mock_oauth(m)
    mock_login(m)
    sf_bulk_connector.JOB_CHECK_TIMER = 0
    mockers.mock_empty_query_res(mock_docall, m)
    
    setdatadir(tempfile.mkdtemp())
    with open(config_file) as conf_file:
        with open('tests/sql/empty_query_res.sql') as job_file:
            run_job_from_file(conf_file, job_file,
                              {'src':'test', 'dst':'test'}, {}, None, None)

@mock.patch.object(sfbulk.callout.Callout, 'docall')
@requests_mock.Mocker()
def test_empty_query_res(mock_docall, m):
    # mock setup    
    mock_oauth(m)
    mock_login(m)
    sf_bulk_connector.JOB_CHECK_TIMER = 0
    mockers.mock_empty_query_res(mock_docall, m)
    
    setdatadir(tempfile.mkdtemp())
    with open(config_file) as conf_file:
        with open('tests/sql/empty_query_res.sql') as job_file:
            run_job_from_file(conf_file, job_file,
                              {'src':'test', 'dst':'test'}, {}, None, None)

# @mock.patch.object(sfbulk.bulk.Bulk, 'job_is_completed')
# @mock.patch.object(sfbulk.callout.Callout, 'docall')
# @requests_mock.Mocker()
# def test_bad_query_res(mock_docall, mock_job_is_completed, m):
#     # mock setup    
#     mock_oauth(m)
#     mock_login(m)
#     sf_bulk_connector.JOB_CHECK_TIMER = 0
#     mockers.mock_bad_response(mock_job_is_completed, mock_docall, m)
    
#     setdatadir(tempfile.mkdtemp())
#     with open(config_file) as conf_file:
#         with open('tests/sql/empty_query_res.sql') as job_file:
#             run_job_from_file(conf_file, job_file,
#                               {'src':'test', 'dst':'test'}, {}, None, None)
            
            
if __name__ == '__main__':
    test_insert_update()
    test_insert_load()
    
