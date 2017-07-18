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
