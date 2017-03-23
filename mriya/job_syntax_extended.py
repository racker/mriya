#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from logging import getLogger
from mriya.job_syntax import BATCH_BEGIN_KEY, BATCH_END_KEY
from mriya.job_syntax import JobSyntax, QUERY_KEY
from mriya.job_syntax import MACRO_KEY, REPLACE_KEY
from mriya.log import loginit, LOG

BATCH_KEY = 'batch'

class JobSyntaxExtended(JobSyntax):

    def __init__(self, main, macroses={}):
        super(JobSyntaxExtended, self).__init__(main)
        # now self.values has parsed items
        #loginit(__name__)

        all_loaded = JobSyntaxExtended.load_all_macro_recursively(
            main, macroses)
        self.values = JobSyntax(all_loaded)
        
        self.values_extended = JobSyntaxExtended.parse_recursive(
            self.values)

    @staticmethod
    def load_all_macro_recursively(lines, macroses):
        """ Inject every noticed macro by its content.
            Nested macro inside of macro are supported. """
        lines = JobSyntax.prepare_lines(lines)
        res = []
        for line in lines:
            parsed_line = JobSyntax([line])
            if not len(parsed_line.items()):
                continue
            item = parsed_line[0]
            assert len(parsed_line.items()) == 1
            if MACRO_KEY in item:
                macro_name = item[MACRO_KEY]
                replaces = {}
                if REPLACE_KEY in item:
                    replaces = item[REPLACE_KEY]
                try:
                    macro = macroses[macro_name]
                except:
                    raise Exception("Could not locate macro: %s.sql" % macro_name)
                macro_lines = JobSyntaxExtended.replace_in_lines(
                    macro, replaces)
                upd = JobSyntaxExtended.load_all_macro_recursively(
                    macro_lines, macroses)
                macro_lines = JobSyntaxExtended.replace_in_lines(
                    upd, replaces)
                res.extend(macro_lines)
            else:
                res.append(line)
        return res

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
                if begin_counter == end_counter:
                    batch_items.append(job_syntax_item)
            
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
