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
    print refname, "retcode=", retcode
    if refname == 'test_dmt':
        assert retcode == 0 
        assert stdout.readlines()[-1] == res
    elif refname == 'test_graph' or refname == 'test_dmt_yes_no':
        assert retcode == 0
    elif refname == 'test_dmt_bad_param':
        assert retcode == 1
    else:
        print stdout.read()

def test_dmt():
    executor = Executor()
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name 'foo1' --dst-name 'foo2' --job-file tests/test.sql --datadir %s" % (tempfile.mkdtemp())
    executor.execute('test_dmt', cmd, input_data=None, output_pipe=True)
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

if __name__ == '__main__':
    loginit(__name__)
    test_dmt_bad_param()
    test_dmt()
