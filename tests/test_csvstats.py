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
from sys import executable as python
from os.path import dirname, join, relpath
from mriya.log import defaultlog
from mriya.csvstats import aggregate_csvs
from mriya.opexecutor import Executor
from mriya.sql_executor import setdatadir, datadir

SQL_SCRIPTS = ['tests/sql/test.sql', 'tests/sql/test2.sql']

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
