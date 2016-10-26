#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import logging
import os
from pprint import PrettyPrinter
from StringIO import StringIO
from random import randint
from configparser import ConfigParser
from mriya.job_syntax import JobSyntax, LINE_KEY
from mriya.job_syntax_extended import JobSyntaxExtended
from mriya.job_syntax import BATCH_PARAMS_KEY
from mriya.sqlite_executor import SqliteExecutor
from mriya.job_controller import JobController
from mriya.bulk_data import get_bulk_data_from_csv_stream
from mriya.log import loginit

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
    query = "SELECT foo, (SELECT 1 WHERE 0=1) as foo2, foo as foo3, \
a.foo4 FROM "
    res = SqliteExecutor.get_query_columns(query)
    try:
        assert(res == ['foo', 'foo2', 'foo3', 'foo4'])
    except:
        print res
        raise

    query = " SELECT Id as id2, \
b.bomba, (SELECT a.Id FROM foo a WHERE a.nope = b.Id) as id__c, \
(SELECT a.Id from foo2 a WHERE a.ggg = b.nnn) as super \
FROM sometable b WHERE ..."
    res = SqliteExecutor.get_query_columns(query)
    try:
        assert(res == ['id2', 'bomba', 'id__c', 'super'])
    except:
        print res
        raise


def test_read():
    text = 'SELECT \\\n\
1 => csv:const1\n\
SELECT 1 => var:MIN => dst:foo'
    test_stream = StringIO(text)
    lines = JobSyntax.prepare_lines(test_stream.readlines())
    assert lines == ['SELECT 1 => csv:const1',
                     'SELECT 1 => var:MIN => dst:foo']


def test_job_syntax():
    lines = ['--something', #will not be added to parsed values
             'SELECT 1 => csv:const1',
             'SELECT 1 => var:MIN',
             'SELECT f1, (SELECT f2 FROM csv.one_ten) as f10 FROM \
csv.one_ten, 9; => csv:final => dst:insert:foo',
             'SELECT 1 as bacth1 from csv.some_csv; \
=> batch_begin:batch1:BATCH',
             'SELECT 1 from dst.some_object WHERE b=a \
=> csv:some_csv => batch_end:BATCH',
             '=> batch_end:BATCH',
             'SELECT 1 as test, 2 as test2; => csv:foo:cache \
=> dst:insert:test_table:new_ids',
             'SELECT 1 as test, 2 as test2; => csv:foo \
=> dst:insert:test_table']
    expected = [
        {},
        {'query': 'SELECT 1', 'csv': 'const1'},
        {'query': 'SELECT 1', 'var': 'MIN'},
        {'query': 'SELECT f1, (SELECT f2 FROM one_ten) as f10 FROM one_ten, 9;',
         'csv': 'final', 'from': 'csv', 'dst' : 'foo', 'op' : 'insert',
         'csvlist': ['one_ten']},
        {'query': 'SELECT 1 as bacth1 from some_csv;',
         'batch_begin': ('batch1', 'BATCH'), 'from': 'csv',
         'csvlist': ['some_csv']},
        {'query': 'SELECT 1 from some_object WHERE b=a',
         'csv': 'some_csv', 'from': 'dst', 'objname': 'some_object',
         'batch_end': 'BATCH'},
        {'query': '', 'batch_end': 'BATCH'},
        {'query': 'SELECT 1 as test, 2 as test2;',
         'op': 'insert', 'dst' : 'test_table', 'csv': 'foo',
         'cache': '', 'new_ids_table': 'new_ids'},
        {'query': 'SELECT 1 as test, 2 as test2;', 'csv': 'foo',
         'op': 'insert', 'dst' : 'test_table'}
    ]

    job_syntax = JobSyntax(lines)
    assert_job_syntax_lines(job_syntax.items(), expected)

def test_var_csv():
    macro_lines = ['SELECT i from csv.ints10000 WHERE i>=CAST(10 as INTEGER) \
LIMIT 2; => batch_begin:i:NESTED',
                   'SELECT {NESTED}; => var:foo2',
                   '=> batch_end:NESTED',
                   "SELECT '{STATIC_VAR}'; => var:static_var"]

    lines = ['SELECT 1; => var:one',
             "SELECT 'csv.ints10000'; => var:CSV_INTS10000 => const:",
             "SELECT * FROM {CSV_INTS10000} LIMIT 1; => var:VAR_0",
             'SELECT "9+0"; => var:nine',
             'SELECT Id FROM src.Account LIMIT 1 => var:sfvar',
             'SELECT {one} as f1, {nine}+1 as f2; => csv:one_ten',
             'SELECT f1, {nine} as f9, (SELECT f2 FROM csv.one_ten) as f10 \
FROM csv.one_ten; => csv:one_nine_ten',
             'SELECT i from csv.ints10000 WHERE i>=2 LIMIT 2; \
=> batch_begin:i:PARAM',
             'SELECT {PARAM}; => var:foo',
             '=> macro:macro_test_batch:STATIC_VAR:something',
             '=> batch_end:PARAM',
             'SELECT {PARAM}; => var:final_test']

    expected = [{'query': 'SELECT 1;', 'var': 'one'},
                {'query': "SELECT 'csv.ints10000';",
                 'var': 'CSV_INTS10000',
                 'csvlist': ['ints10000'],
                 'const': '',
                 'from': 'csv'},
                {'query': "SELECT * FROM {CSV_INTS10000} LIMIT 1;",
                 'var': 'VAR_0'},
                {'query': 'SELECT "9+0";', 'var': 'nine'},
                {'query': 'SELECT Id FROM Account LIMIT 1', 
                 'var': 'sfvar', 'from': 'src', 'objname': 'Account'},
                {'query': 'SELECT {one} as f1, {nine}+1 as f2;',
                 'csv': 'one_ten'},
                {'query': 'SELECT f1, {nine} as f9, (SELECT f2 FROM \
one_ten) as f10 FROM one_ten;',
                 'csvlist': ['one_ten'],
                 'csv': 'one_nine_ten',
                 'from': 'csv'},
                {   'batch': [   {   'line': 'SELECT {PARAM}; => var:foo',
                                     'query': 'SELECT {PARAM};',
                                     'var': 'foo'},
                                 {   'batch': [   {   'line': 'SELECT {NESTED}; => var:foo2',
                                                      'query': 'SELECT {NESTED};',
                                                      'var': 'foo2'}],
                                     'batch_begin': ('i', 'NESTED'),
                                     'csvlist': ['ints10000'],
                                     'from': 'csv',
                                     'line': 'SELECT i from csv.ints10000 WHERE i>=CAST(10 as INTEGER) LIMIT 2; => batch_begin:i:NESTED',
                                     'query': 'SELECT i from ints10000 WHERE i>=CAST(10 as INTEGER) LIMIT 2;'},
                                 {   'line': "SELECT 'something'; => var:static_var",
                                     'query': "SELECT 'something';",
                                     'var': 'static_var'}],
                    'batch_begin': ('i', 'PARAM'),
                    'csvlist': ['ints10000'],
                    'from': 'csv',
                    'query': 'SELECT i from ints10000 WHERE i>=2 LIMIT 2;'},
                {'query': 'SELECT {PARAM};', 'var': 'final_test'}
            ]

    job_syntax_extended = JobSyntaxExtended(
        lines, {'macro_test_batch': macro_lines })
    assert_job_syntax_lines(job_syntax_extended.items(), expected)
    try:
        os.remove('one_nine_ten.csv')
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
    with open('one_nine_ten.csv') as resulted_file:
        assert resulted_file.read() == 'f1,f9,f10\n1,9,10\n'

def test_job_controller():
    notch = randint(0, 1000000)
    print "notch", notch
    lines = ["SELECT Id,Account_Birthday__c,Name FROM src.Account LIMIT 2; \
=> csv:some_data:cache",
             "SELECT Id from csv.some_data LIMIT 1; => var:id_test",
             "SELECT Account_Birthday__c,Name FROM csv.some_data; \
=> csv:some_data_staging => dst:insert:Account:newids",
             "UPDATE csv.some_data SET Account_Birthday__c=null, Name='%d'; \
             SELECT Id,Account_Birthday__c,Name FROM csv.some_data \
WHERE Id = '{id_test}' \
             => csv:some_data_staging => dst:update:Account" % notch]
    job_syntax = JobSyntaxExtended(lines)
    with open(config_filename) as conf_file:
        job_controller = JobController(conf_file.name, endpoint_names,
                                       job_syntax, {}, False)
    job_controller.run_job()
    del job_controller
    with open('some_data_staging.csv') as resulted_file:
        csv_data = get_bulk_data_from_csv_stream(resulted_file)
        name_idx = csv_data.fields.index('Name')
        assert 1 == len(csv_data.rows)
        assert csv_data.rows[0][name_idx] == str(notch)
    with open('newids.csv') as newids_file:
        csv_data = get_bulk_data_from_csv_stream(newids_file)
        id_idx = csv_data.fields.index('Id')
        assert 2 == len(csv_data.rows)
        for row in csv_data.rows:
            assert len(row[id_idx]) >= 15


if __name__ == '__main__':
    loginit(__name__)
    test_columns()
    test_read()
    test_job_syntax()
    test_var_csv()
    test_job_controller()

