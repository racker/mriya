#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import os.path
from logging import getLogger
from configparser import ConfigParser
from mriya.log import loginit
from mriya.job_syntax import *
from mriya.opexecutor import Executor
from mriya.sql_executor import SqlExecutor
from mriya.sqlite_executor import SqliteExecutor
from mriya.salesforce_executor import SalesforceExecutor
from mriya.data_connector import create_bulk_connector
from mriya.bulk_data import csv_from_bulk_data, parse_batch_res_data
from mriya.job_syntax_extended import JobSyntaxExtended, BATCH_KEY

INTS_TABLE = SqlExecutor.csv_name('ints10000')

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

    def __init__(self, config_filename, endpoint_names,
                 job_syntax, variables, debug_steps):
        loginit(__name__)
        self.config_file = open(config_filename)
        self.config = ConfigParser()
        self.config.read_file(self.config_file)
        self.job_syntax = job_syntax
        self.endpoints = Endpoints(self.config, endpoint_names)
        self.variables = variables
        self.debug_steps = debug_steps
        self.external_exec = Executor()
        # create csv file for an internal batch purpose
        with open(INTS_TABLE, 'w') as ints:
            ints.write('i\n')
            for i in xrange(10001):
                ints.write('%d\n' % i)


    def __del__(self):
        del self.endpoints
        del self.external_exec

    def create_executor(self, job_syntax_item):
        sql_exec = None
        sqltype = JobSyntax.sqltype(job_syntax_item)
        getLogger(__name__).debug(job_syntax_item)
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
        if job_syntax_item and QUERY_KEY in job_syntax_item:
            query = job_syntax_item[QUERY_KEY].strip()
            is_csv = CSV_KEY in job_syntax_item
            is_var = VAR_KEY in job_syntax_item
            is_cache = CACHE_KEY in job_syntax_item
            if not query:
                return
            if not is_var and is_csv and is_cache:
                csv_name = SqlExecutor.csv_name(job_syntax_item[CSV_KEY])
                csv_size = SqlExecutor.csv_size(job_syntax_item[CSV_KEY])
                if csv_size:
                    getLogger(__name__).info(
                        "SKIP query: '%s', csvfile exist: %s",
                        query, csv_name)
                    return
            sql_exec = self.create_executor(job_syntax_item)
            try:
                sql_exec.execute()
            except:
                print job_syntax_item
                raise
            sql_exec.saved_csv()
            self.post_operation(job_syntax_item)
            self.variables = sql_exec.variables
            del sql_exec

    def step_by_step(self):
        yes = set(['yes','y', 'ye', ''])
        no = set(['no','n'])
        choice = raw_input().lower()
        if choice in yes:
            return True
        elif choice in no:
            return False
        else:
            print "Please respond with 'yes' or 'no'"

    def run_job(self):
        batch_items = None
        batch = None
        for job_syntax_item in self.job_syntax:
            if not job_syntax_item:
                continue
            if self.debug_steps:
                print "NEXT SQL:", SqlExecutor.prepare_query_put_vars(
                    job_syntax_item['line'], self.variables)
                print "continue execution? y/n"
                if not self.step_by_step():
                    exit(0)
            if BATCH_KEY in job_syntax_item:
                self.run_batch_(job_syntax_item)
            else:
                self.handle_job_item_(job_syntax_item)

    def run_batch_(self, job_syntax_item):
        # run batch_begin query and save list of batches to var
        self.handle_job_item_(job_syntax_item)
        # run batch itself
        batch_param_name = job_syntax_item[BATCH_BEGIN_KEY][1]
        batch_syntax_items = job_syntax_item[BATCH_KEY]
        batch_params = self.variables[BATCH_PARAMS_KEY]
        getLogger(__name__).info("batch params list %s",
                                 batch_params )
        # loop through batch parameters list
        if not batch_params:
            getLogger(__name__).info("Skip empty batch %s",
                                     batch_param_name)
            return
        for param in batch_params:
            self.variables[batch_param_name] = param
            getLogger(__name__).info("------ batch %s/%s",
                                     param, batch_params)
            getLogger(__name__)\
                .info("set batch var: %s=%s",
                      batch_param_name, param )
            # prepare variables for external batch
            external_vars = {}
            print self.variables
            for key, val in self.variables.iteritems():
                if type(val) is not list:
                    external_vars[key] = val
            #run batches sequentially
            #self.external_exec.wait_for_complete()
            if 0:
                self.run_external_batch(param, self.config_file,
                                        batch_syntax_items, external_vars)
            else:
                self.run_internal_batch(param, self.config_file,
                                        batch_syntax_items, external_vars)
        #res = self.external_exec.wait_for_complete()

    def run_internal_batch(self, _, config_filename,
                           job_syntax_items, variables):
        batch_job = JobController(self.config_file.name,
                                  self.endpoints.endpoint_names,
                                  job_syntax_items,
                                  variables,
                                  self.debug_steps)
        batch_job.run_job()
        del batch_job

    def run_external_batch(self, batch_param, config_file,
                           job_syntax_items, variables):
        text_vars = ''
        for key, val in variables.iteritems():
            text_vars += ' --var %s %s' % (key, val)
        cmd_fmt = 'python mriya_dmt.py --conf-file {conf_file} \
--job-stdin --src-name "{src}" --dst-name "{dst}" {variables}'
        cmd = cmd_fmt.format(conf_file=config_file.name,
                             src=self.endpoints.endpoint_names['src'],
                             dst=self.endpoints.endpoint_names['dst'],
                             variables=text_vars)
        input_data = self.batch_input_text_data(job_syntax_items)
        getLogger(__name__).info('Invoke cmd:%s', cmd)
        self.external_exec.execute('batch_%s' % batch_param, cmd,
                                   input_data=input_data)

    def batch_input_text_data(self, job_syntax_items):
        res = ''
        for item in job_syntax_items:
            if item:
                res += item[LINE_KEY] + '\n'
        return res

    def handle_transmitter_op(self, job_syntax_item, endpoint):
        opname = job_syntax_item[OP_KEY]
        csv_data = None
        csv_filename = SqlExecutor.csv_name(job_syntax_item[CSV_KEY])
        with open(csv_filename) as csv_f:
            i = 0
            csv_data = csv_f.readlines()
        num_lines = len(csv_data)
        # do nothing for empty data set
        if num_lines <= 1:
            getLogger(__name__).info('skip empty csv')
            stub = ['"Id","Success","Created","Error"\n']
            result_ids = parse_batch_res_data(stub)
        else:
            objname = job_syntax_item[endpoint]
            conn = self.endpoints.endpoint(endpoint)
            max_batch_size = job_syntax_item[BATCH_SIZE_KEY]
            getLogger(__name__).info('EXECUTE: op:%s, Csv data size=%d',
                                     opname, len(csv_data))
            if opname == OP_UPDATE and len(csv_data):
                res = conn.bulk_update(objname, csv_data,
                                       max_batch_size)
                result_ids = parse_batch_res_data(res)
            if opname == OP_DELETE and len(csv_data):
                res = conn.bulk_delete(objname, csv_data,
                                       max_batch_size)
                result_ids = parse_batch_res_data(res)
            elif opname == OP_INSERT and len(csv_data):
                res = conn.bulk_insert(objname, csv_data,
                                       max_batch_size)
                result_ids = parse_batch_res_data(res)
        if NEW_IDS_TABLE in job_syntax_item:
            results_file_name = \
                 SqlExecutor.csv_name(job_syntax_item[NEW_IDS_TABLE])
            with open(results_file_name, 'w') as result_ids_file:
                csv_data = csv_from_bulk_data(result_ids)
                result_ids_file.write(csv_data)
            getLogger(__name__).info('Saved result ids: %s',
                                     results_file_name)
        getLogger(__name__).info('Done: %s operation', opname)


    def post_operation(self, job_syntax_item):
        endpoint = None
        if DST_KEY in job_syntax_item:
            endpoint = DST_KEY
        if SRC_KEY in job_syntax_item:
            endpoint = SRC_KEY

        if endpoint:
            opname = job_syntax_item[OP_KEY]
            if opname == OP_UPSERT or \
               opname == OP_INSERT or \
               opname == OP_DELETE or \
               opname == OP_UPDATE:
                self.handle_transmitter_op(job_syntax_item, endpoint)
            else:
                getLogger(__name__).error('Unsupported operation: %s',
                                          opname)
                assert(0)


