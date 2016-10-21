#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from logging import getLogger
from mriya.job_syntax import BATCH_BEGIN_KEY, BATCH_END_KEY
from mriya.job_syntax import JobSyntax, QUERY_KEY
from mriya.job_syntax import MACRO_KEY, REPLACE_KEY
from mriya.log import loginit

BATCH_KEY = 'batch'

class JobSyntaxExtended(JobSyntax):

    def __init__(self, main, macroses={}):
        super(JobSyntaxExtended, self).__init__(main)
        # now self.values has parsed items
        loginit(__name__)
        self.values = JobSyntaxExtended.integrate_macros_into_job_items(
            self.values, macroses)
        self.values_extended = JobSyntaxExtended.parse_recursive(
            self.values)

    def __iter__(self):
        for lst in self.values_extended:
            yield lst

    def __getitem__(self, idx):
        return self.values_extended[idx]

    def items(self):
        return self.values_extended

    @staticmethod
    def replace_in_lines(lines, replaces):
        if replaces:
            res = []
            for line in lines:
                for replace_key, replace_value in replaces.iteritems():
                    line = line.replace('{%s}' % replace_key,
                                        replace_value)
                res.append(line)
        else:
            res = lines
        return res

    @staticmethod
    def integrate_macros_into_job_items(job_syntax_items, macroses):
        if macroses:
            res = []
            for item in job_syntax_items:
                if MACRO_KEY in item:
                    macro_name = item[MACRO_KEY]
                    if REPLACE_KEY in item:
                        macro_lines = JobSyntaxExtended.replace_in_lines(
                            macroses[macro_name],
                            item[REPLACE_KEY])
                    else:
                        macro_lines = macroses[macro_name]
                    macro_job_syntax_lines = JobSyntax(macro_lines)
                    res.extend(macro_job_syntax_lines)
                else:
                    res.append(item)
        else:
            res = job_syntax_items
        return res

    def batch_var_name(self, watch_batch_var, job_syntax_item):
        if BATCH_BEGIN_KEY in job_syntax_item and not watch_batch_var:
            watch_batch_var = job_syntax_item[BATCH_BEGIN_KEY][1]
        elif BATCH_END_KEY in job_syntax_item and \
             job_syntax_item[BATCH_END_KEY] == watch_batch_var:
            watch_batch_var = None
        return watch_batch_var
        
    @staticmethod
    def parse_recursive(self_values):
        res = []
        batch_items = []
        begin_counter = 0
        end_counter = 0
        for job_syntax_item in self_values:
            if BATCH_BEGIN_KEY in job_syntax_item:
                begin_counter += 1
            if BATCH_END_KEY in job_syntax_item:
                end_counter += 1
            
            if begin_counter > end_counter:
                batch_items.append(job_syntax_item)
            elif begin_counter == end_counter and begin_counter != 0:
                # add all saved items, 
                # skip first batch_begin, last batch_end
                nested = JobSyntaxExtended.parse_recursive(
                    batch_items[1:-1])
                batch = batch_items[0]
                batch[BATCH_KEY] = nested
                res.append(batch)
                del batch_items[:]
                begin_counter = 0
                end_counter = 0
            elif begin_counter != 0 or end_counter != 0:
                assert(0)
            else:
                res.append(job_syntax_item)
        return res
