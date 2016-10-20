#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
from logging import getLogger
from mriya.job_syntax import JobSyntax, CSV_KEY
from mriya.log import loginit

class SqlExecutor(object):
    def __init__(self, job_syntax_item, variables):
        loginit(__name__)
        self.job_syntax_item = job_syntax_item
        self.variables = variables

    def execute(self, query):
        raise NotImplementedError("'execute' method not defined!")

    @staticmethod
    def csv_name(table_name):
        return table_name + '.csv'

    @staticmethod
    def csv_size(table_name):
        if os.path.isfile(SqlExecutor.csv_name(table_name)):
            return os.stat(SqlExecutor.csv_name(table_name)).st_size
        else:
            return None

    def saved_csv(self):
        if CSV_KEY in self.job_syntax_item:
            name = self.job_syntax_item[CSV_KEY]
            getLogger(__name__).info('Saved csv: %s, size: %d',
                                     self.csv_name(name),
                                     self.csv_size(name))

    @staticmethod
    def prepare_query_put_vars(query, variables):
        for var_name, var_value in variables.iteritems():
            if type(var_value) != list:
                query = query.replace('{%s}' % (var_name), var_value)
        return query

    def save_var(self, key, value):
        self.variables[key] = value
        getLogger(__name__).info("set var: %s=%s", key, value)
