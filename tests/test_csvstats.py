#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016-2017, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from os.path import dirname, join
from mriya.log import defaultlog
from mriya.csvstats import aggregate_csvs
from mriya.opexecutor import Executor

SQL_SCRIPTS = ['tests/test.sql', 'tests/test2.sql']

def run_mriya():
    executor = Executor()
    jobs = ''
    for script in SQL_SCRIPTS:
        jobs += ' --job-file %s' % script
    cmd = "python mriya_dmt.py --conf-file test-config.ini --src-name 'foo1' --dst-name 'foo2' --datadir data %s" % jobs
    executor.execute('test_dmt', cmd, input_data=None, output_pipe=True)
    executor.wait_for_complete()

def test_graph():
    # run scripts
    run_mriya()
    # and then evaluate csv files
    csvdir='../data'
    graph_fname = './tests/test_graph'
    csvdirpath = join(dirname(graph_fname), csvdir)
    res = aggregate_csvs(sqlscripts=SQL_SCRIPTS,
                         csvdirpath=csvdirpath)
    assert res['aggregation_test'] == 10000
    assert res['aggregation_test2'] == 10000    
    assert res['test_fields_table'] == 0

if __name__ == '__main__':
    defaultlog()
    test_graph()
