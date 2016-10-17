#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from logging import getLogger
from mriya.job_syntax import BATCH_BEGIN_KEY, BATCH_END_KEY
from mriya.job_syntax import JobSyntax, QUERY_KEY
from mriya.log import loginit

BATCH_KEY = 'batch'

class JobSyntaxExtended(JobSyntax):

    def __init__(self, raw_lines):
        super(JobSyntaxExtended, self).__init__(raw_lines)
        loginit(__name__)
        self.values_extended = self.parse_lines_extended(self.values)

    def __iter__(self):
        for lst in self.values_extended:
            yield lst

    def __getitem__(self, idx):
        return self.values_extended[idx]

    def items(self):
        return self.values_extended

    def batch_var_name(self, watch_batch_var, job_syntax_item):
        if BATCH_BEGIN_KEY in job_syntax_item and not watch_batch_var:
            watch_batch_var = job_syntax_item[BATCH_BEGIN_KEY][1]
        elif BATCH_END_KEY in job_syntax_item and \
             job_syntax_item[BATCH_END_KEY] == watch_batch_var:
            watch_batch_var = None
        return watch_batch_var
        
    def parse_lines_extended(self, self_values):
        values = []
        batch_items = []
        watch_batch = None
        for job_syntax_item in self_values:
            print job_syntax_item
            if not watch_batch:
                values.append(job_syntax_item)
            elif not (BATCH_END_KEY in job_syntax_item and \
                      job_syntax_item[BATCH_END_KEY] == watch_batch):
                # add all related to batch except end batch flag
                batch_items.append(job_syntax_item)
            existing_batch = watch_batch
            watch_batch = self.batch_var_name(watch_batch, job_syntax_item)
            # if all batch items are located
            if existing_batch and not watch_batch:
                values[-1][BATCH_KEY] = batch_items
        return values

