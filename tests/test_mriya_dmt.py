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

import tempfile
import logging
import os
from pprint import PrettyPrinter
from random import randint
from configparser import ConfigParser
from mriya import bulk_data
from mriya.job_syntax import JobSyntax, LINE_KEY
from mriya.job_syntax_extended import JobSyntaxExtended
from mriya.job_syntax import BATCH_PARAMS_KEY
from mriya.sql_executor import SqlExecutor
from mriya.job_controller import JobController
from mriya.bulk_data import get_bulk_data_from_csv_stream
from mriya.bulk_data import prepare_received_sf_data
from mriya.log import loginit, STDOUT, STDERR, LOG
from mriya.sf_bulk_connector import SfBulkConnector
from mriya.opexecutor import Executor

res="info=pen,pineapple,apple,pen\n"

def observer(refname, retcode, stdout):
    exit0 = ['test_graph', 'test_dmt_yes_no', 'test_macro']
    exit1 = ['test_dmt_bad_param', 'test_batch_param_error',
             'test_assert_type_error', 'test_assert_zero', 'test_assert_nonzero',
             'test_unsupported_csv_prefix', 'test_cant_locate_macro_error',
             'test_transmitter_name_error', 'test_transmitter_value_error',
             'test_bad_operation_error', 'test_macro_param_error'
    ]
    print refname, "retcode=", retcode
    if refname in exit0:
        assert retcode == 0
    if refname in exit1:
        assert retcode == 1
    elif refname == 'test_dmt':
        assert retcode == 0 
        assert stdout.readlines()[-1] == res
    else:
        print stdout.read()

def create_symbolic_link(name):
    cmd = 'ln -s /dev/stdin %s' % name
    executor = Executor(silent_exit=True)    
    executor.execute('test_dmt_yes_no', cmd, input_data=None, output_pipe=False)
        
def test_dmt():
    create_symbolic_link('tests/dev_stdin')
    executor = Executor()
    stdin_data = """
SELECT "SELECT 'pen' as field1, 'pineapple' as field2, 'apple' as field3, 'pen' as field4" => var:QUERY => const:

=> macro:macro_test \
   :RES_TABLE_NAME:test_fields_table

SELECT * from csv.ints10000 => csv:aggregation_test

-- put following line just to test some branches in graph.py
SELECT fake FROM src.fake => csv:aggregation_test:cache
SELECT fake FROM dst.fake2 => csv:aggregation_test:cache

SELECT * FROM csv.test_fields_table => var:info:publish
"""
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name 'foo1' --dst-name 'foo2' --job-file tests/dev_stdin --datadir %s" % (tempfile.mkdtemp())
    executor.execute('test_dmt', cmd, input_data=stdin_data, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res

def test_stepbystep_yes_maybe_no():
    answers = """y
y
maybe_yes_maybe_no_maybe_xxx_idontknow
n

"""
    executor = Executor()
    cmd = "python mriya_dmt.py --conf-file test-config.ini --step-by-step --src-name 'foo1' --dst-name 'foo2' --job-file tests/test.sql --datadir %s" % (tempfile.mkdtemp())
    executor.execute('test_dmt_yes_no', cmd, input_data=answers, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res

def run_test_graph(datadir, sqlfpath):
    """it's will be used by another test test_job_reader.test_job_controller
    As all the data was prepared by mocked sources and mocks will not work in a subprocess"""
    # run graph
    executor = Executor()
    graphpath = 'tests/test_graph'
    graphdir = os.path.dirname(graphpath)
    relative_path = os.path.relpath(tempfile.mkdtemp(), graphdir)
    cmd = "python graph_dmt.py --conf-file test-config.ini --job-file %s --job-file tests/test2.sql --save-graph %s --csvdir %s" % (sqlfpath, graphpath, relative_path)
    print relative_path
    executor.execute('test_graph', cmd, input_data=None, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res
    
def test_dmt_bad_param():
    executor = Executor(silent_exit=True)
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name 'foo1' --dst-name 'foo2'"
    executor.execute('test_dmt_bad_param', cmd, input_data=None, output_pipe=True)
    executor.poll_for_complete(observer)

def test_assert_zero():
    executor = Executor(silent_exit=True)
    stdin_data = """
SELECT count() FROM csv.ints10000 WHERE i>0=>var:ASSERT_SHOULD_FAIL=>assert:zero
"""
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name 'foo1' --dst-name 'foo2' --job-file /dev/stdin --datadir %s" % (tempfile.mkdtemp())
    executor.execute('test_assert_zero', cmd, input_data=stdin_data, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res

def test_assert_nonzero():
    executor = Executor(silent_exit=True)
    stdin_data = "SELECT 0 =>var:ASSERT_SHOULD_FAIL=>assert:nonzero"
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name 'foo1' --dst-name 'foo2' --job-file /dev/stdin --datadir %s" % (tempfile.mkdtemp())
    executor.execute('test_assert_nonzero', cmd, input_data=stdin_data, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res

def test_assert_type_error():
    executor = Executor(silent_exit=True)
    stdin_data = "SELECT 0 =>var:ASSERT=>assert:unsupported_assert"
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name 'foo1' --dst-name 'foo2' --job-file /dev/stdin --datadir %s" % (tempfile.mkdtemp())
    executor.execute('test_assert_type_error', cmd, input_data=stdin_data, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res

    
def test_unsupported_csv_prefix():
    executor = Executor(silent_exit=True)
    stdin_data = """
SELECT 'csv.' => var:CSV_PREFIX
-- such prefix doesn't supported
SELECT * FROM {CSV_PREFIX}ints10000 LIMIT 10 => csv:newints10
"""
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name 'foo1' --dst-name 'foo2' --job-file /dev/stdin --datadir %s" % (tempfile.mkdtemp())
    executor.execute('test_unsupported_csv_prefix', cmd, input_data=stdin_data, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res

def test_transmitter_name_error():
    executor = Executor(silent_exit=True)
    stdin_data = "SELECT 'csv.' => VAR:VAR"
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name 'foo1' --dst-name 'foo2' --job-file /dev/stdin --datadir %s" % (tempfile.mkdtemp())
    executor.execute('test_transmitter_name_error', cmd, input_data=stdin_data, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res
    
def test_transmitter_value_error():
    executor = Executor(silent_exit=True)
    stdin_data = "SELECT 'csv.' => var"
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name 'foo1' --dst-name 'foo2' --job-file /dev/stdin --datadir %s" % (tempfile.mkdtemp())
    executor.execute('test_transmitter_value_error', cmd, input_data=stdin_data, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res

def test_batch_param_error():
    executor = Executor(silent_exit=True)
    stdin_data = """
-- required param ids result table is omited, will raise an error on parsing stage
SELECT Id FROM src.Acount => csv:forsubmit => src:insert:Account__c:200"""
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name test --dst-name test --job-file /dev/stdin --datadir %s" % (tempfile.mkdtemp())
    executor.execute('test_batch_param_error', cmd, input_data=stdin_data, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res

def test_bad_operation_error():
    executor = Executor(silent_exit=True)
    stdin_data = "SELECT Id FROM src.Acount => csv:forsubmit => src:badop:Account__c:200:resids"
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name test --dst-name test --job-file /dev/stdin --datadir %s" % (tempfile.mkdtemp())
    executor.execute('test_bad_operation_error', cmd, input_data=stdin_data, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res

def test_macro_param_error():
    executor = Executor(silent_exit=True)
    stdin_data = """
-- QUERY value itself is ommited (actually query should not be passed ar macro param)
=> macro:macro_test:QUERY"""
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name test --dst-name test --job-file /dev/stdin --datadir %s" % (tempfile.mkdtemp())
    executor.execute('test_bad_operation_error', cmd, input_data=stdin_data, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res

def test_cant_locate_macro_error():
    executor = Executor(silent_exit=True)
    stdin_data = "=> macro:macro_cant_locate"
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name test --dst-name test --job-file /dev/stdin --datadir %s" % (tempfile.mkdtemp())
    executor.execute('test_cant_locate_macro_error', cmd, input_data=stdin_data, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res
    
def test_macro():
    create_symbolic_link('tests/dev_stdin')
    executor = Executor(silent_exit=True)
    stdin_data = """
=> macro:macro_test:QUERY:SELECT 1:RES_TABLE_NAME:test
=> macro:macro_no_params
"""
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name test --dst-name test --job-file tests/dev_stdin --datadir %s" % (tempfile.mkdtemp())
    executor.execute('test_macro', cmd, input_data=stdin_data, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res
    
    
if __name__ == '__main__':
    loginit(__name__)
    test_dmt_bad_param()
    test_dmt()
