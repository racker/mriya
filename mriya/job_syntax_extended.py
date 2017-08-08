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

    # not used
    # def __getitem__(self, idx):
    #     return self.values_extended[idx]

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
            # it's boring to write unit test to cover following 2 lines, so comment it
            # elif begin_counter != 0 or end_counter != 0:
            #     assert(0)
            else:
                res.append(job_syntax_item)
        return res
