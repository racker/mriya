#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import logging
import os
from StringIO import StringIO
from random import randint
from mriya.job_syntax import JobSyntax
from mriya.sqlite_executor import SqliteExecutor
from mriya.job_controller import JobController
from mriya.log import loginit

config_filename = 'test-config.ini'
endpoint_names = {'dst': 'test', 'src': 'test'}

UAT_SECTION = 'uat'

def test_read():
    text = 'SELECT \\\n\
1 => csv:const1\n\
SELECT 1 => var:MIN => dst:foo'
    test_stream = StringIO(text)
    lines = JobSyntax.prepare_lines(test_stream.readlines())
    assert lines == ['SELECT 1 => csv:const1',
                     'SELECT 1 => var:MIN => dst:foo']


def test_parse():
    lines = ['SELECT 1 => csv:const1',
             'SELECT 1 => var:MIN',
             'SELECT f1, (SELECT f2 FROM csv.one_ten) as f10 FROM csv.one_ten, 9; => csv:final => dst:insert:foo',
             'SELECT 1 as bacth1 from csv.some_csv; => batch_begin:batch1:BATCH',
             'SELECT 1 from dst.some_object WHERE b=a => csv:some_csv => batch_end:',
             ]
    expected = [
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
         'batch_end': ''}
    ]
    assert len(lines) == len(expected)
    for idx in xrange(len(lines)):
        res = JobSyntax.parse_line(lines[idx])
        exp = expected[idx]
        logging.getLogger(__name__).info('idx: %d, res=%s', idx, res)
        assert res == exp

def test_var_csv():
    lines = ['SELECT 1; => var:one',
             'SELECT 9; => var:nine',
             'SELECT {one} as f1, {nine}+1 as f2; => csv:one_ten',
             'SELECT f1, {nine} as f9, (SELECT f2 FROM csv.one_ten) as f10 FROM csv.one_ten; => csv:one_nine_ten']
    job_syntax = JobSyntax(lines)
    try:
        os.remove('one_nine_ten.csv')
    except:
        pass
    job_controller = JobController(config_filename,
                                   endpoint_names,
                                   job_syntax)
    job_controller.run_job()
    del job_controller
    with open('one_nine_ten.csv') as resulted_file:
        assert resulted_file.read() == 'f1,f9,f10\n1,9,10\n'

def test_job_controller():
    notch = randint(0, 1000000)
    lines = ["SELECT Id,Account_Birthday__c,Name FROM src.Account WHERE Id = '001n0000009bI3MAAU' => csv:some_data",
             "\
UPDATE csv.some_data SET Account_Birthday__c=null, Name='%d' WHERE Id='001n0000009bI3MAAU'; \
SELECT Id,Account_Birthday__c,Name FROM csv.some_data WHERE Id = '001n0000009bI3MAAU'; \
=> csv:some_data_staging => dst:update:Account" % notch]
    job_syntax = JobSyntax(lines)
    job_controller = JobController(config_filename,
                                   endpoint_names,
                                   job_syntax)
    job_controller.run_job()
    del job_controller
    expected_file = "Id,Account_Birthday__c,Name\n001n0000009bI3MAAU,#N/A,%d\n" % notch
    with open('some_data_staging.csv') as resulted_file:
        assert expected_file == resulted_file.read()

if __name__ == '__main__':
    loginit(__name__)
    test_read()
    test_parse()
    test_var_csv()
    test_job_controller()

