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

import mriya
import logging
import os
from pprint import PrettyPrinter
from StringIO import StringIO
from random import randint
from configparser import ConfigParser
from mriya import bulk_data
from mriya.job_syntax import JobSyntax, LINE_KEY
from mriya.job_syntax_extended import JobSyntaxExtended
from mriya.job_syntax import BATCH_PARAMS_KEY
from mriya.sql_executor import SqlExecutor, setdatadir, datadir
from mriya.job_controller import JobController
from mriya.bulk_data import get_bulk_data_from_csv_stream
from mriya.bulk_data import prepare_received_sf_data
from mriya.log import loginit, STDOUT, STDERR, LOG
from mriya.sf_bulk_connector import SfBulkConnector
from mriya import sf_bulk_connector
from test_mriya_dmt import run_test_graph
import tempfile

config_filename = 'test-config.ini'
endpoint_names = {'dst': 'test', 'src': 'test'}

def assert_job_syntax_lines(res_syntax_items, expected):
    try:
        assert len(res_syntax_items) == len(expected)
    except:
        PrettyPrinter(indent=4).pprint(res_syntax_items)
        raise
    for idx in xrange(len(res_syntax_items)):
        res = res_syntax_items[idx]
        if res:
            del res[LINE_KEY]
        exp = expected[idx]
        try:
            assert res == exp
            logging.getLogger(__name__).info('OK idx: %d', idx)
        except:
            logging.getLogger(__name__).info('FAILED idx: %d', idx)
            PrettyPrinter(indent=4).pprint(res)
            raise


def test_columns():
    loginit(__name__)
    query = "SELECT foo, (SELECT 1 WHERE 0=1) as foo2, foo as foo3, \
a.foo4 FROM "
    res = SqlExecutor.get_query_columns(query)
    try:
        assert(res == ['foo', 'foo2', 'foo3', 'foo4'])
    except:
        print res
        raise

    query = " SELECT Id as id2, \
b.bomba, (SELECT a.Id FROM foo a WHERE a.nope = b.Id) as id__c, \
(SELECT a.Id from foo2 a WHERE a.ggg = b.nnn) as super \
FROM sometable b WHERE ..."
    res = SqlExecutor.get_query_columns(query)
    try:
        assert(res == ['id2', 'bomba', 'id__c', 'super'])
    except:
        print res
        raise


def test_read():
    loginit(__name__)
    text = 'SELECT \\\n\
1 => csv:const1\n\
SELECT 1 => var:MIN => dst:foo'
    test_stream = StringIO(text)
    lines = JobSyntax.prepare_lines(test_stream.readlines())
    assert lines == ['SELECT 1 => csv:const1',
                     'SELECT 1 => var:MIN => dst:foo']


def test_job_syntax():
    setdatadir(tempfile.mkdtemp())
    loginit(__name__)
    lines = ['--something', #this is a comment line, will not be added to parsed values
             'SELECT 1 => csv:const1',
             'SELECT 1 => var:MIN',
             'SELECT f1, (SELECT f2 FROM csv.one_ten) as f10 FROM \
csv.one_ten, 9; => csv:final:cache => dst:insert:foo:1:res => type:sequential',
             'SELECT 1 as bacth1 from csv.some_csv; \
=> batch_begin:batch1:BATCH',
             'SELECT 1 from dst.some_object WHERE b=a \
=> csv:some_csv => batch_end:BATCH',
             '=> batch_end:BATCH',
             'SELECT 1 as test, 2 as test2; => csv:foo:cache \
=> dst:insert:test_table:1:new_ids',
             'SELECT 1 as test, 2 as test2; => csv:foo \
=> dst:insert:test_table:1:res']
    expected = [
        {'query': 'SELECT 1', 'csv': 'const1'},
        {'from': 'csv', 'query': 'SELECT 1', 'var': 'MIN'},
        {'query': 'SELECT f1, (SELECT f2 FROM one_ten) as f10 FROM one_ten, 9;',
         'csv': 'final', 'from': 'csv', 'dst' : 'foo', 'op' : 'insert',
         'type': 'sequential', 'cache': '',
         'csvlist': ['one_ten'], 'batch_size': '1', 'new_ids_table': 'res'},
        {'query': 'SELECT 1 as bacth1 from some_csv;',
         'batch_begin': ('batch1', 'BATCH'), 'from': 'csv',
         'csvlist': ['some_csv']},
        {'query': 'SELECT 1 from some_object WHERE b=a',
         'csv': 'some_csv', 'from': 'dst', 'objname': 'some_object',
         'batch_end': 'BATCH'},
        {'query': '', 'batch_end': 'BATCH'},
        {'query': 'SELECT 1 as test, 2 as test2;',
         'op': 'insert', 'dst' : 'test_table', 'csv': 'foo',
         'cache': '', 'new_ids_table': 'new_ids',
         'batch_size': '1'},
        {'query': 'SELECT 1 as test, 2 as test2;', 'csv': 'foo',
         'op': 'insert', 'dst' : 'test_table',
         'batch_size': '1', 'new_ids_table': 'res'}
    ]

    job_syntax = JobSyntax(lines)
    assert_job_syntax_lines(job_syntax.items(), expected)

@mock.patch.object(sfbulk.callout.Callout, 'docall')
@requests_mock.Mocker()
def test_var_csv(mock_docall, m):
    # mock setup
    sf_bulk_connector.JOB_CHECK_TIMER = 0    
    mockers.mock_var_csv(mock_docall, m)
    # test itself
    setdatadir(tempfile.mkdtemp())    
    loginit(__name__)
    print "test_var_csv"
    macro_lines = ['SELECT i from csv.ints10000 WHERE i>=CAST(10 as INTEGER) \
LIMIT 2; => batch_begin:i:NESTED',
                   'SELECT {NESTED}; => var:foo2',
                   '=> batch_end:NESTED',
                   "SELECT '{STATIC_VAR}'; => var:static_var"]

    lines = ['SELECT 1; => var:one',
             "SELECT 'csv.ints10000'; => var:CSV_INTS10000 => const:",
             "SELECT * FROM {CSV_INTS10000} LIMIT 1; => var:VAR_0",
             'SELECT "9+0"; => var:nine',
             'SELECT Id FROM src.Account LIMIT 1 => csv:sfvar',
             'SELECT * FROM csv.sfvar => var:sfvar',
             'SELECT {one} as f1, {nine}+1 as f2; => csv:one_ten',
             "SELECT 'one_nine_ten' => var:ONENINETEN",             
             'SELECT f1, {nine} as f9, (SELECT f2 FROM csv.one_ten) as f10 \
FROM csv.one_ten; => csv:{ONENINETEN}',
             'SELECT i from csv.ints10000 WHERE i>=2 LIMIT 2; \
=> batch_begin:i:PARAM',
             'SELECT {PARAM}; => var:foo',
             '=> macro:macro_test_batch:STATIC_VAR:something',
             '=> batch_end:PARAM',
             'SELECT {PARAM}; => var:final_test']

    expected = [{'from': 'csv', 'query': 'SELECT 1;', 'var': 'one'},
                {'from': 'csv',
                 'query': "SELECT 'csv.ints10000';",
                 'var': 'CSV_INTS10000',
                 'const': '',
                 'from': 'csv'},
                {'from': 'csv',
                 'query': "SELECT * FROM {CSV_INTS10000} LIMIT 1;",
                 'var': 'VAR_0'},
                {'from': 'csv', 'query': 'SELECT "9+0";', 'var': 'nine'},
                {'objname': 'Account','query': 'SELECT Id FROM Account LIMIT 1', 
                 'csv': 'sfvar', 'from': 'src', 'objname': 'Account'},
                {'query': 'SELECT * FROM sfvar', 'var': 'sfvar',
                 'csvlist': ['sfvar'], 'from': 'csv'},

                {'query': 'SELECT {one} as f1, {nine}+1 as f2;',
                 'csv': 'one_ten'},
                {'from': 'csv', 'query': "SELECT 'one_nine_ten'", 'var': 'ONENINETEN'},
                {'query': 'SELECT f1, {nine} as f9, (SELECT f2 FROM \
one_ten) as f10 FROM one_ten;',
                 'csvlist': ['one_ten'],
                 'csv': '{ONENINETEN}',
                 'from': 'csv'},
                {   'batch': [   {   'line': 'SELECT {PARAM}; => var:foo',
                                     'query': 'SELECT {PARAM};',
                                     'var': 'foo',
                                     'from': 'csv'},
                                 {   'batch': [   {   'line': 'SELECT {NESTED}; => var:foo2',
                                                      'query': 'SELECT {NESTED};',
                                                      'var': 'foo2',
                                                      'from': 'csv'}],
                                     'batch_begin': ('i', 'NESTED'),
                                     'csvlist': ['ints10000'],
                                     'from': 'csv',
                                     'line': 'SELECT i from csv.ints10000 WHERE i>=CAST(10 as INTEGER) LIMIT 2; => batch_begin:i:NESTED',
                                     'query': 'SELECT i from ints10000 WHERE i>=CAST(10 as INTEGER) LIMIT 2;'},
                                 {   'line': "SELECT 'something'; => var:static_var",
                                     'query': "SELECT 'something';",
                                     'var': 'static_var',
                                     'from': 'csv'}],
                    'batch_begin': ('i', 'PARAM'),
                    'csvlist': ['ints10000'],
                    'from': 'csv',
                    'query': 'SELECT i from ints10000 WHERE i>=2 LIMIT 2;'},
                {'from': 'csv', 'query': 'SELECT {PARAM};', 'var': 'final_test'}
            ]

    job_syntax_extended = JobSyntaxExtended(
        lines, {'macro_test_batch': macro_lines })
    assert_job_syntax_lines(job_syntax_extended.items(), expected)
    try:
        os.remove(SqlExecutor.csv_name('one_nine_ten'))
    except:
        pass
    with open(config_filename) as conf_file:
        job_controller = JobController(conf_file.name,
                                       endpoint_names,
                                       job_syntax_extended,
                                       {}, False)
    job_controller.run_job()
    res_batch_params = job_controller.variables[BATCH_PARAMS_KEY]
    assert res_batch_params == ['2', '3']
    sfvar = job_controller.variables['sfvar']
    assert len(sfvar) >= 15
    final_param = job_controller.variables['final_test']
    assert final_param == '3'
    del job_controller
    with open(SqlExecutor.csv_name('one_nine_ten')) as resulted_file:
        assert resulted_file.read() == 'f1,f9,f10\n1,9,10\n'

@mock.patch.object(sfbulk.callout.Callout, 'docall')
@requests_mock.Mocker()
def test_job_controller(mock_docall, m):
    # mock setup
    sf_bulk_connector.JOB_CHECK_TIMER = 0    
    mockers.mock_job_controller(mock_docall, m)
    # test itself
    setdatadir(tempfile.mkdtemp())
    # test debug coverage
    mriya.log.INITIALIZED_LOGGERS = {}
    mriya.log.LOGGING_LEVEL = logging.DEBUG
    loginit(__name__)
    print "test_job_controller"
    
    # prepare test_csv.csv
    test_csv = ['"Alexa__c"', '"hello\n\n2"']
    with open(SqlExecutor.csv_name('test_csv'), "w") as test_csv_f:
        test_csv_f.write('"Alexa__c"\n"hello<N CR><N CR>2"\n')

    notch = 'test1234567'
    with open('tests/sql/complicated.sql') as sql_f:
        lines = sql_f.readlines()
    job_syntax = JobSyntaxExtended(lines)
    with open(config_filename) as conf_file:
        job_controller = JobController(conf_file.name, endpoint_names,
                                       job_syntax, {}, False)
    job_controller.run_job()
    del job_controller
    # check resulted data
    with open(SqlExecutor.csv_name('some_data_staging')) as resulted_file:
        csv_data = get_bulk_data_from_csv_stream(resulted_file)
        name_idx = csv_data.fields.index('Name')
        assert 1 == len(csv_data.rows)
        assert csv_data.rows[0][name_idx] == notch
    with open(SqlExecutor.csv_name('newids')) as newids_file:
        csv_data = get_bulk_data_from_csv_stream(newids_file)
        id_idx = csv_data.fields.index('Id')
        try:
            assert 1 == len(csv_data.rows)
        except:
            print "len(csv_data.rows)", len(csv_data.rows)
            raise
        for row in csv_data.rows:
            assert len(row[id_idx]) >= 15

    assert open(SqlExecutor.csv_name('test_csv')).read() \
        == open(SqlExecutor.csv_name('test_csv_2')).read()
    # run another test using created data
    run_test_graph(datadir(), "tests/sql/complicated.sql")
    
def test_batch_splitter():
    loginit(__name__)
    batch_ranges = SfBulkConnector.batch_ranges(10, 3)
    print batch_ranges
    assert(batch_ranges == [(0,2), (3,5), (6,8), (9,9)])

if __name__ == '__main__':
    loginit(__name__)
    # test_batch_splitter()
    # test_columns()
    # test_read()
    # test_job_syntax()
    test_var_csv()
    # test_job_controller()

