#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from logging import getLogger
from mriya.job_syntax import JobSyntax
from mriya.log import loginit

class SqlExecutor(object):
    def __init__(self, job_syntax_item, variables):
        loginit(__name__)
        self.job_syntax_item = job_syntax_item
        self.variables = variables

    def execute(self, query):
        raise NotImplementedError("'execute' method not defined!")

    def prepare_query_put_vars(self, query):
        for var_name, var_value in self.variables.iteritems():
            if type(var_value) != list:
                query = query.replace('{%s}' % (var_name), var_value)
        return query

    def save_var_as_query_results(self, key, value):
        self.variables[key] = value
        getLogger(__name__).info("set var: %s=%s", key, value)
