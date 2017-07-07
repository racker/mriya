#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

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
datadir = '/tmp/csvdata'

def observer(refname, retcode, stdout):
    print refname, "retcode=", retcode
    if refname == 'test_dmt':
        assert retcode == 0 
        assert stdout.readlines()[-1] == res
    elif refname == 'test_graph':
        assert retcode == 0
    elif refname == 'test_dmt_bad_param':
        assert retcode == 1
    else:
        print stdout.read()

def test_dmt():
    executor = Executor()
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name 'foo1' --dst-name 'foo2' --job-file tests/test.sql --datadir %s" % (datadir)
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
    cmd = "python mriya_dmt.py --conf-file test-config.ini --step-by-step --src-name 'foo1' --dst-name 'foo2' --job-file tests/test.sql --datadir %s" % (datadir)
    executor.execute('test_dmt_yes_no', cmd, input_data=answers, output_pipe=True)
    res = executor.poll_for_complete(observer)
    print res
    
def test_graph():
    executor = Executor()
    graphpath = 'tests/test_graph'
    graphdir = os.path.dirname(graphpath)
    relative_path = os.path.relpath(datadir, graphdir)
    cmd = "python graph_dmt.py --conf-file test-config.ini --job-file tests/test.sql --job-file tests/test2.sql --save-graph %s --csvdir %s" % (graphpath, relative_path)
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
