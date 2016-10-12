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
from mriya.job_syntax import BATCH_BEGIN_KEY, BATCH_END_KEY, BATCH_PARAMS_KEY
from mriya.job_syntax import NEW_IDS_TABLE
from mriya.sqlite_executor import SqliteExecutor
from mriya.salesforce_executor import SalesforceExecutor
from mriya.data_connector import create_bulk_connector
from mriya.bulk_data import csv_from_bulk_data, parse_batch_res_data

class Endpoints(object):
    def __init__(self, config, endpoint_names):
        self.config = config
        self.endpoint_names = endpoint_names
        self.endpoints = {}

    def __del__(self):
        del self.endpoints

    def ensure_endpoint_exist(self, endpoint_name):
        setting_name = self.endpoint_names[endpoint_name]
        # create endpoint if not created yet
        if endpoint_name not in self.endpoints:
            self.endpoints[endpoint_name] = create_bulk_connector(
                self.config, setting_name)

    def endpoint(self, name):
        self.ensure_endpoint_exist(name)
        return self.endpoints[name]

class JobController(object):

    def __init__(self, config, endpoint_names, job_syntax):
        loginit(__name__)
        self.config = config
        self.job_syntax = job_syntax
        self.endpoints = Endpoints(config, endpoint_names)
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

    def handle_job_item_(self, job_syntax_item):
        if job_syntax_item:
            sql_exec = self.create_executor(job_syntax_item)
            sql_exec.execute()
            self.post_operation(job_syntax_item)
            self.variables = sql_exec.variables
            del sql_exec

    def run_job(self):
        batch_items = None
        batch = None
        for job_syntax_item in self.job_syntax:
            if not job_syntax_item:
                continue
            if not batch:
                self.handle_job_item_(job_syntax_item)
            else:
                if batch_items is None:
                    batch_items = []
                batch_items.append(job_syntax_item)
            batch_param_name = batch
            batch = self.get_batch_param_name(batch, job_syntax_item)
            # if all batch items are located
            if batch_param_name and not batch:
                self.run_batch_(batch_items, batch_param_name)

    def run_batch_(self, batch_items, batch_param_name):
        batch_params = self.variables[BATCH_PARAMS_KEY]
        # loop through batch parameters list
        for param in batch_params:
            self.variables[batch_param_name] = param
            for job_syntax_item in batch_items:
                self.handle_job_item_(job_syntax_item)

    def get_batch_param_name(self, batch, job_syntax_item):
        if BATCH_BEGIN_KEY in job_syntax_item:
            batch = job_syntax_item[BATCH_BEGIN_KEY][1]
        elif BATCH_END_KEY in job_syntax_item:
            batch = None
        return batch

    def post_operation(self, job_syntax_item):
        endpoint = None
        if DST_KEY in job_syntax_item:
            endpoint = DST_KEY
        if SRC_KEY in job_syntax_item:
            endpoint = SRC_KEY

        if endpoint:
            opname = job_syntax_item[OP_KEY]
            if opname == OP_UPSERT or opname == OP_INSERT or \
               opname == OP_UPDATE:
                csv_data = None
                csv_filename = job_syntax_item[CSV_KEY] + '.csv'
                with open(csv_filename) as csv_f:
                    csv_data = csv_f.read()
                objname = job_syntax_item[endpoint]
                conn = self.endpoints.endpoint(endpoint)
                getLogger(__name__).info('EXECUTE: op:%s, Csv data size=%d',
                                         opname, len(csv_data))
                if opname == OP_UPDATE:
                    conn.bulk_update(objname, csv_data)
                elif opname == OP_INSERT:
                    res = conn.bulk_insert(objname, csv_data)
                    result_ids = parse_batch_res_data(res)
                    results_file_name = job_syntax_item[NEW_IDS_TABLE] \
                                        + '.csv'
                    with open(results_file_name, 'w') as result_ids_file:
                        csv_data = csv_from_bulk_data(result_ids)
                        result_ids_file.write(csv_data)
                getLogger(__name__).info('Done: %s operation', opname)
            else:
                getLogger(__name__).error('Unsupported operation: %s',
                                          opname)
                assert(0)

