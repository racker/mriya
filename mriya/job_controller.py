#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"


from logging import getLogger
from mriya.log import loginit
from mriya.job_syntax import JobSyntax
from mriya.job_syntax import SQL_TYPE_SQLITE, SQL_TYPE_SF
from mriya.job_syntax import FROM_KEY, OP_KEY, DST_KEY, SRC_KEY, CSV_KEY
from mriya.job_syntax import OP_UPSERT, OP_INSERT, OP_UPDATE
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
        loginit(__name__)
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
            self.post_operation(job_syntax_item)
            self.variables = sql_exec.variables
            del sql_exec

    def post_operation(self, job_syntax_item):
        print job_syntax_item
        endpoint = None
        if DST_KEY in job_syntax_item:
            endpoint = DST_KEY
        if SRC_KEY in job_syntax_item:
            endpoint = SRC_KEY

        if endpoint and \
           (job_syntax_item[OP_KEY] == OP_UPSERT or \
            job_syntax_item[OP_KEY] == OP_INSERT or \
            job_syntax_item[OP_KEY] == OP_UPDATE):
            csv_data = None
            csv_filename = job_syntax_item[CSV_KEY] + '.csv'
            with open(csv_filename) as csv_f:
                csv_data = csv_f.read()
            objname = job_syntax_item[endpoint]
            conn = self.endpoints.endpoint(endpoint)
            getLogger(__name__).info('objname %s, csv %s',
                                     objname, csv_data)
            if job_syntax_item[OP_KEY] == OP_UPDATE:
                conn.bulk_update(objname, csv_data)
            getLogger(__name__).info('Done: %s operation',
                                     job_syntax_item[OP_KEY])

