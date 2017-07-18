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

from multiprocessing import Process
from multiprocessing import Pipe
from collections import namedtuple
from collections import deque

_EOF = 'EOF'
_ERROR_OCCURED = "OpMultiprocessingErrorOccured"

def _worker(pipes):
    """ worker wrapper to be used with FastQueueProcessor
    pipes -- tuple with (read, write) pipes"""
    pipe_work, pipe_main = pipes
    worker_func = None
    worker_1st_arg = None
    while True:
        try:
            if not worker_func:
                worker_func = pipe_work.recv()
                worker_1st_arg = pipe_work.recv()
            else:
                # Read from the output pipe and do nothing
                arg = pipe_work.recv()
                # close pipe if 'EOF' received
                if arg == _EOF:
                    pipe_work.close()
                    pipe_main.close()
                    break
                else:
                    res = worker_func(worker_1st_arg, arg) #pylint: disable=E1102
                    pipe_work.send(res)
        except EOFError:
            break
        except:
            pipe_work.send(_ERROR_OCCURED)
            raise

def _create_worker_proccess(worker, worker_1st_arg):
    """ Launch worker process and send via pipe a worker function 'worker'
    and initial argument 'worker_1st_arg'. Return worker descriptor.
    worker -- worker function is expected to have two arguments :
    1st - initial data, 2nd - data object for processing.
    It must return result of data processing.
    worker_1st_arg -- initial arg to worker function"""
    pipe_work, pipe_main = Pipe()
    proc = Process(target=_worker, args=((pipe_work, pipe_main), ))
    proc.start()
    pipe_main.send(worker)
    pipe_main.send(worker_1st_arg)
    return FastQueueProcessor.FastProc(pipe_work=pipe_work,
                                       pipe_main=pipe_main,
                                       proc=proc)

class FastQueueProcessor:
    """ Implementation of asynchronous pipe that easy to use for parallel
    calculations. Write data into pipe then read from pipe result of data
    processeing. Supports multiple parallel workers which are competing for
    data in queue. First read first serve."""
    FastProc = namedtuple('FastProc', ['pipe_work', 'pipe_main', 'proc'])

    def __init__(self, worker, worker_1st_arg, procn):
        self.queue_data = deque()
        self.procs = [_create_worker_proccess(worker, worker_1st_arg) \
                      for _ in xrange(procn)]
        self.proc_statuses = [False for i in xrange(procn)]
        self.error = False

    def _consume_from_queue(self):
        """ try to consume data from queue by all available workers
        which are ready to consume data """
        for i in xrange(len(self.proc_statuses)):
            if not self.count():
                break
            status = self.proc_statuses[i]
            if not status:
                data = self.queue_data.popleft()
                self.procs[i].pipe_main.send(data)
                self.proc_statuses[i] = True

    def is_any_working(self):
        """ return True if any worker is not yet finished data processing """
        for status in self.proc_statuses:
            if status:
                return True
        return False

    def __del__(self):
        """ Send EOF signal to all workers, close pipes and
        wait while workers are done."""
        for proc in self.procs:
            proc.pipe_main.send(_EOF)
            proc.pipe_work.close()
            proc.pipe_main.close()
            proc.proc.join()

    def count(self):
        """ return data count in queue """
        return len(self.queue_data)

    def put(self, data):
        """ Put data to queue, then try to consume datas from queue
        by all available workers which are eager for data"""
        self.queue_data.append(data)
        self._consume_from_queue()

    def poll(self):
        """ return True/False is it results available or not """
        for proc in self.procs:
            if proc.pipe_main.poll():
                return True
        return False


    def get(self):
        """ return result available in pipe """
        res = None
        while True:
            if not len(self.queue_data) and not self.is_any_working():
                break
            for i in xrange(len(self.procs)):
                proc = self.procs[i]
                if proc.pipe_main.poll():
                    res = proc.pipe_main.recv()
                    self.proc_statuses[i] = False
                    if res == _ERROR_OCCURED:
                        res = None
                        self.error = True
                    break
            if res is not None:
                break
            self._consume_from_queue()
        return res
