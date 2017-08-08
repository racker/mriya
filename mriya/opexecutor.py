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

import collections
from subprocess import Popen, PIPE

class Executor:
    def __init__(self, silent_exit = False):
        self.subprocesses = []
        self.silent_exit = silent_exit

    def __del__(self):
        self.wait_for_complete()

    def handle_exit(self, proc):
        retcode = proc.popen.poll()
        if retcode is None:
            return None
        if retcode != 0 and not self.silent_exit:
            raise Exception('Opexecutor',
                            'Exit code=%d,  %s' % (retcode, str(proc.cmd)))
        return retcode
       
    def free_completed(self):
        for i in reversed(range(len(self.subprocesses))):
            res = self.handle_exit(self.subprocesses[i])
            if res is not None:
                del(self.subprocesses[i])

    def poll_for_complete(self, observer):
        results = {}
        completed = []
        while True and len(completed) < len(self.subprocesses):
            for p in self.subprocesses:
                retcode = p.popen.poll()
                if p.popen.pid not in completed and retcode is not None:
                    completed.append(p.popen.pid)
                    if observer:
                       res = observer(p.refname, retcode, p.popen.stdout)
                       results[p.refname] = res
        self.handle_exit(p)
        return results

    def wait_for_complete(self):
        for p in self.subprocesses:
            retcode = p.popen.wait()
            self.handle_exit(p)

    def execute(self, refname, cmd, input_data=None, output_pipe=False):
        tclass = collections.namedtuple('Proc', ['refname', 'popen', 'cmd'])
        self.free_completed()
        stdout_flag = None
        if output_pipe:
            stdout_flag = PIPE
        if input_data:
            try_count = 3
            while try_count:  # as sometimes getting "IOError: [Errno 32] Broken pipe"
                try:
                    proc = Popen(cmd, shell=True, stdin=PIPE, stdout=stdout_flag)
                    proc.stdin.write(input_data)
                    proc.stdin.close()
                    try_count = 0 # successfull
                except IOError:
                    try_count = try_count -1
        else:
            proc = Popen(cmd, shell=True, stdout=stdout_flag)
        self.subprocesses.append(tclass(refname=refname, popen=proc, cmd=cmd))
