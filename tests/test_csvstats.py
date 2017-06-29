#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016-2017, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import tempfile
from sys import executable as python
from os.path import dirname, join, relpath
from mriya.log import defaultlog
from mriya.csvstats import aggregate_csvs
from mriya.opexecutor import Executor
from mriya.sql_executor import setdatadir, datadir

SQL_SCRIPTS = ['tests/test.sql', 'tests/test2.sql']

def run_mriya(datadir):
    executor = Executor()
    jobs = []
    for script in SQL_SCRIPTS:
        jobs.append('--job-file')
        jobs.append(script)
    cmdl = ['python', 'mriya_dmt.py', '--conf-file', 'test-config.ini',
           '--src-name', 'foo1', '--dst-name', 'foo2',
           '--datadir', datadir]
    cmdl.extend(jobs)
    cmd = ' '.join(cmdl)
    print cmd
    executor.execute('test_dmt', cmd, input_data=None, output_pipe=True)
    executor.wait_for_complete()

def test_graph():
    setdatadir(tempfile.mkdtemp())
    run_mriya(datadir())
    sqlsdir = './tests'
    print "sqlsdir", sqlsdir
    relative_path = relpath(datadir(), sqlsdir)
    print "relpath", relative_path
    datapath = join(sqlsdir, relative_path)
    print "datapath", datapath
    res = aggregate_csvs(sqlscripts=SQL_SCRIPTS,
                         csvdirpath=datapath)
    print res
    assert res['aggregation_test'] == 10001
    assert res['aggregation_test2'] == 10001
    assert res['test_fields_table'] == 1

    
if __name__ == '__main__':
    defaultlog()
    test_graph()
