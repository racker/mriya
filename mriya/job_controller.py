#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"


from mriya.job_syntax import JobSyntax
from mriya.job_syntax import SQL_TYPE_SQLITE, SQL_TYPE_SF
from mriya.job_syntax import FROM_KEY, OP_KEY, DST_KEY, SRC_KEY
from mriya.job_syntax import OP_VALUE_UPSERT
from mriya.sqlite_executor import SqliteExecutor
from mriya.salesforce_executor import SalesforceExecutor
from mriya.data_connector import create_bulk_connector


class Endpoints(object):
    def __init__(self, config_filename, endpoint_names):
        self.config_filename = config_filename
        self.endpoint_names = endpoint_names
        self.endpoints = {}

    def __del__(self):
        del self.endpoints

    def ensure_endpoint_exist(self, endpoint_name):
        setting_name = self.endpoint_names[endpoint_name]
        # create endpoint if not created yet
        if endpoint_name not in self.endpoints:
            self.endpoints[endpoint_name] = create_bulk_connector(
                self.config_filename, setting_name)

    def endpoint(self, name):
        self.ensure_endpoint_exist(name)
        return self.endpoints[name]

class JobController(object):

    def __init__(self, config_filename, endpoint_names, job_syntax):
        self.config_filename = config_filename
        self.job_syntax = job_syntax
        self.endpoints = Endpoints(config_filename, endpoint_names)
        self.variables = {}

    def __del__(self):
        del self.endpoints

    def create_executor(self, job_syntax_item):
        sql_exec = None
        sqltype = JobSyntax.sqltype(job_syntax_item)
        if sqltype == SQL_TYPE_SQLITE:
            sql_exec = SqliteExecutor(job_syntax_item, self.variables)
        elif sqltype == SQL_TYPE_SF:
            key_from = job_syntax_item[FROM_KEY]
            conn = self.endpoints.endpoint(key_from)
            sql_exec = SalesforceExecutor(conn,
                                          job_syntax_item,
                                          self.variables)
        return sql_exec

    def run_job(self):
        for job_syntax_item in self.job_syntax:
            sql_exec = self.create_executor(job_syntax_item)
            sql_exec.execute()
            self.variables = sql_exec.variables
            del sql_exec

    def post_operation(self, job_syntax_item):
        if job_syntax_item[DST_KEY] or job_syntax_item[SRC_KEY]:
            if job_syntax_item[OP_KEY] == 'upsert':
                pass

