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
    elif refname == 'test_dmt_bad_param':
        assert retcode == 1
    else:
        print stdout.read()

def test_dmt():
    executor = Executor()
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name 'foo1' --dst-name 'foo2' --job-file tests/test.sql"
    executor.execute('test_dmt', cmd, input_data=None, output_pipe=True)
    executor.poll_for_complete(observer)

def test_dmt_bad_param():
    executor = Executor(silent_exit=True)
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name 'foo1' --dst-name 'foo2'"
    executor.execute('test_dmt_bad_param', cmd, input_data=None, output_pipe=True)
    executor.poll_for_complete(observer)

if __name__ == '__main__':
    loginit(__name__)
    test_dmt_bad_param()
    test_dmt()
