#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"


from mriya.job_syntax import JobSyntax
from mriya.job_syntax import SQL_TYPE_SQLITE, SQL_TYPE_SF
from mriya.job_syntax import FROM_KEY
from mriya.sqlite_executor import SqliteExecutor
from mriya.salesforce_executor import SalesforceExecutor
from mriya.data_connector import create_bulk_connector

class JobController(object):

    def __init__(self, config_filename, endpoint_names, job_syntax):
        self.config_filename = config_filename
        self.job_syntax = job_syntax
        self.endpoints = [endpoint_names, {}]
        self.variables = {}

    def __del__(self):
        del self.endpoints[1]

    def create_executor(self, job_syntax_item):
        sql_exec = None
        sqltype = JobSyntax.sqltype(job_syntax_item)
        if sqltype == SQL_TYPE_SQLITE:
            sql_exec = SqliteExecutor(job_syntax_item, self.variables)
        elif sqltype == SQL_TYPE_SF:
            key_from = job_syntax_item[FROM_KEY]
            name_from = self.endpoints[0][key_from]
            if key_from not in self.endpoints[1]:
                self.endpoints[1][key_from] = create_bulk_connector(
                    self.config_filename, name_from)
            sql_exec = SalesforceExecutor(self.endpoints[1][key_from],
                                          job_syntax_item,
                                          self.variables)
        return sql_exec

    def run_job(self):
        for job_syntax_item in self.job_syntax:
            sql_exec = self.create_executor(job_syntax_item)
            sql_exec.execute()
            self.variables = sql_exec.variables
            del sql_exec
