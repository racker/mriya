#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import re
from logging import getLogger
from mriya.job_syntax import JobSyntax, CSV_KEY, QUERY_KEY
from mriya.job_syntax import DST_KEY, SRC_KEY, PUBLISH_KEY
from mriya.log import loginit, LOG, STDOUT, MOREINFO

# real data path must be set dynamically
DATADIRNAME = None

def setdatadir(datadir):
    global DATADIRNAME
    DATADIRNAME = datadir

def datadir():
    return DATADIRNAME

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
            name = self.job_syntax_item[CSV_KEY]
            getLogger(MOREINFO).info('Saved csv: %s, size: %d',
                                   self.csv_name(name),
                                   self.csv_size(name))

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
