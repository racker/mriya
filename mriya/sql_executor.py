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

import os
import re
from logging import getLogger
from mriya.job_syntax import JobSyntax, CSV_KEY, QUERY_KEY
from mriya.job_syntax import DST_KEY, SRC_KEY, PUBLISH_KEY
from mriya.job_syntax import ASSERT_KEY, ASSERT_ZERO, ASSERT_NONZERO
from mriya.log import loginit, LOG, STDOUT, STDERR, MOREINFO

# real data path must be set dynamically
DATADIRNAME = None

def setdatadir(datadir):
    global DATADIRNAME
    DATADIRNAME = datadir

def datadir():
    return DATADIRNAME

def var_replaced(variables, job_syntax_item, key):
    """ Get job syntax item's value. If value is variable then replace by var value and return """
    val = job_syntax_item[key]
    return SqlExecutor.prepare_query_put_vars(val, variables)

class SqlExecutor(object):
    def __init__(self, job_syntax_item, variables):
        self.job_syntax_item = job_syntax_item
        self.variables = variables

    def execute(self, query):
        raise NotImplementedError("'execute' method not defined!")

    @staticmethod
    def csv_name(table_name):
        return os.path.join(datadir(), table_name) + '.csv'

    @staticmethod
    def valid_csv_exist(table_name):
        fname = SqlExecutor.csv_name(table_name)
        try:
            with open(fname) as f:
                if f.readline().find('<result-list xmlns=') == -1:
                    return True
                else:
                    return False
        except:
            return False
    
    @staticmethod
    def csv_size(table_name):
        if os.path.isfile(SqlExecutor.csv_name(table_name)):
            return os.stat(SqlExecutor.csv_name(table_name)).st_size
        else:
            return None

    def saved_csv(self):
        if CSV_KEY in self.job_syntax_item:
            csv_key_val = var_replaced(
                self.variables, self.job_syntax_item, CSV_KEY)
            getLogger(MOREINFO).info('Saved csv: %s, size: %d',
                                   self.csv_name(csv_key_val),
                                   self.csv_size(csv_key_val))

    @staticmethod
    def get_query_var_names(query):
        import re
        return re.findall(r'\{(.*?)\}',query)
            
    @staticmethod
    def prepare_query_put_vars(query, variables):
        for var_name, var_value in variables.iteritems():
            if type(var_value) != list:
                query = query.replace('{%s}' % (var_name), var_value)
        return query

    def save_var(self, key, value):
        if len(value) and value[0] == '"' and value[-1] == '"':
            value = value[1:-1]
        self.variables[key] = value
        getLogger(LOG).info("set var: %s=%s", key, value)
        if PUBLISH_KEY in self.job_syntax_item:
            getLogger(STDOUT).info("%s=%s", key, value)
        if ASSERT_KEY in self.job_syntax_item:
            assert_type = self.job_syntax_item[ASSERT_KEY]
            if assert_type == ASSERT_ZERO:
                if int(value) != 0:
                    getLogger(STDERR).error('Assert 0: %s variable value %s should be 0' % \
                                            (key, value))
                    exit(1)
            elif assert_type == ASSERT_NONZERO:
                if int(value) == 0:
                    getLogger(STDERR).error('Assert non 0: %s variable value should not be 0' \
                                            % (key))
                    exit(1)
            # error can't be handled here as it was handled before in job_syntax.py
            # else:
            #     getLogger(STDERR).error('Bad assertion type: %s' % (assert_type))
            #     exit(1)

    @staticmethod
    def get_sub_str_between(query, start_str, end_str):
        query = query.lower()
        start = query.find(start_str) + len(start_str)
        end = query.find(end_str)
        return query[start:end]

    @staticmethod
    def get_query_columns(query):
        regex = r'\(.*?\)'
        query = re.sub(regex, '_', query)
        cols = SqlExecutor.get_sub_str_between(query,
                                                  'select', 'from')
        res = [x.strip().split(' ')[-1] for x in cols.split(',')]
        return [x.split('.')[-1] for x in res]
