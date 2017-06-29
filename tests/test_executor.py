#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import tempfile
from os.path import join
from mriya.opexecutor import Executor

def async_engine_assert(stdin_pipe_data):
    expected_order = ['test4.2', 'test4.1']
    actual_order = []
    def observer(refname, retcode, output):
        stdout_data = None
        if output:
            stdout_data = output.read()
        print refname, retcode, stdout_data
        actual_order.append(refname)

    executor = Executor()
    cmd = 'echo "test1" && sleep 1 && echo first done'
    executor.execute('test4.1', cmd, stdin_pipe_data)
    cmd = 'echo "test2" && echo second done'
    executor.execute('test4.2', cmd, stdin_pipe_data)
    print "waiting..."
    executor.poll_for_complete(observer)
    print expected_order
    print actual_order
    assert expected_order == actual_order
    del executor

def test_1():
    async_engine_assert(None)

def test_2():
    datadir = tempfile.mkdtemp()
    async_engine_assert(join(datadir, 'foo'))

def test_3():
    executor = Executor()
    executor.execute('t1', 'echo "test1" && sleep 1 && echo first done')
    executor.execute('t2', 'echo "test2" && echo first done')
    executor.wait_for_complete()
