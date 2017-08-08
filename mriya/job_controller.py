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
import time
import os.path
from logging import getLogger
from configparser import ConfigParser
from mriya import sql_executor
from mriya.log import loginit, LOG, STDOUT
from mriya.job_syntax import *
from mriya.opexecutor import Executor
from mriya.sql_executor import SqlExecutor, var_replaced
from mriya.sqlite_executor import SqliteExecutor
from mriya.salesforce_executor import SalesforceExecutor
from mriya.data_connector import create_bulk_connector
from mriya.bulk_data import csv_from_bulk_data, parse_batch_res_data
from mriya.bulk_data import BulkData
from mriya.job_syntax_extended import BATCH_KEY
from mriya.sf_merge import SoapMerge
from mriya.config import *

INTS_TABLE = 'ints10000'

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
        #loginit(__name__)
        self.config = None
        if config_filename:
            self.config_file = open(config_filename)
            self.config = ConfigParser()
            self.config.read_file(self.config_file)
        self.job_syntax = job_syntax
        self.endpoints = Endpoints(self.config, endpoint_names)
        self.variables = variables
        self.debug_steps = debug_steps
        # create csv file for an internal batch purpose
        ints1000_csv = SqlExecutor.csv_name(INTS_TABLE)
        with open(ints1000_csv, 'w') as ints:
            ints.write('i\n')
            for i in xrange(10001):
                ints.write('%d\n' % i)
            ints.flush()

    def __del__(self):
        del self.endpoints

    def create_executor(self, job_syntax_item):
        sql_exec = None
        sqltype = JobSyntax.sqltype(job_syntax_item)
        getLogger(LOG).debug(job_syntax_item)
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
            getLogger(LOG).info(job_syntax_item)
            if not is_var and is_csv and is_cache:
                csv_key_val = var_replaced(self.variables, job_syntax_item, CSV_KEY)
                csv_name = SqlExecutor.csv_name(csv_key_val)
                csv_size = SqlExecutor.csv_size(csv_key_val)
                if csv_size and SqlExecutor.valid_csv_exist(csv_key_val):
                    getLogger(LOG).info(
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
        while True:
            choice = raw_input().lower()
            print "choice", choice
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
            if self.debug_steps:
                print "NEXT SQL:", SqlExecutor.prepare_query_put_vars(
                    job_syntax_item['line'], self.variables)
                print "continue execution? y/n"
                if not self.step_by_step():
                    exit(0)
            if BATCH_KEY in job_syntax_item:
                self.run_in_loop(job_syntax_item)
            else:
                self.handle_job_item_(job_syntax_item)

    def run_in_loop(self, job_syntax_item):
        # run batch_begin query and save list of batches to var
        self.handle_job_item_(job_syntax_item)
        # run batch itself
        batch_param_name = job_syntax_item[BATCH_BEGIN_KEY][1]
        batch_syntax_items = job_syntax_item[BATCH_KEY]
        batch_params = self.variables[BATCH_PARAMS_KEY]
        getLogger(LOG).info("batch params list %s",
                                 batch_params )
        # loop through batch parameters list
        if not batch_params:
            getLogger(STDOUT).info("Skip empty batch %s",
                                batch_param_name)
            return
        for param in batch_params:
            self.variables[batch_param_name] = param
            getLogger(STDOUT).info("------ loop %s/%s",
                                param, batch_params)
            getLogger(LOG)\
                .info("set batch var: %s=%s",
                      batch_param_name, param )
            # prepare variables for external batch
            external_vars = {}
            for key, val in self.variables.iteritems():
                if type(val) is not list:
                    external_vars[key] = val
            #run batches sequentially
            self.run_loop_procedure(param, self.config_file,
                                    batch_syntax_items, external_vars)

    def run_loop_procedure(self, _, config_filename,
                           job_syntax_items, variables):
        batch_job = JobController(self.config_file.name,
                                  self.endpoints.endpoint_names,
                                  job_syntax_items,
                                  variables,
                                  self.debug_steps)
        batch_job.run_job()
        del batch_job

    def csvdata(self, filename):
        csv_data = None
        with open(filename) as csv_f:
            csv_data = csv_f.readlines()
        return csv_data

    def handle_transmitter_op(self, job_syntax_item, endpoint):
        opname = job_syntax_item[OP_KEY]
        # run batches sequentially / parallel
        if BATCH_TYPE_KEY in job_syntax_item:
            if job_syntax_item[BATCH_TYPE_KEY] == BATCH_TYPE_PARALLEL_KEY:
                batch_seq = False
            elif job_syntax_item[BATCH_TYPE_KEY] == BATCH_TYPE_SEQUENTIAL_KEY:
                batch_seq = True
            else:
                getLogger(STDERR).error('Unknown batch type: %s', job_syntax_item[BATCH_TYPE_KEY])
                exit(1)
        else:
            batch_seq = False # parallel by default
        csv_key_val = var_replaced(self.variables, job_syntax_item, CSV_KEY)
        csv_filename = SqlExecutor.csv_name(csv_key_val)
        csv_data = self.csvdata(csv_filename)
        num_lines = len(csv_data)
        # do nothing for empty data set
        if num_lines <= 1:
            getLogger(LOG).info('skip empty csv')
            stub = ['"Id","Success","Created","Error"\n']
            result_ids = parse_batch_res_data(stub)
        else:
            objname = job_syntax_item[endpoint]
            conn = self.endpoints.endpoint(endpoint)
            max_batch_size = int(job_syntax_item[BATCH_SIZE_KEY])
            getLogger(STDOUT).info('EXECUTE: %s %s, lines count=%d',
                                     opname, objname, num_lines-1)
            t_before = time.time()
            if len(csv_data):
                if opname == OP_UPDATE:
                    res = conn.bulk_update(objname, csv_data,
                                           max_batch_size, batch_seq)
                elif opname == OP_DELETE:
                    res = conn.bulk_delete(objname, csv_data,
                                           max_batch_size, batch_seq)
                elif opname == OP_INSERT:
                    res = conn.bulk_insert(objname, csv_data,
                                           max_batch_size, batch_seq)
                else:
                    getLogger(STDERR).error("Operation '%s' isn't supported" % opname)
                    exit(1)

                result_ids = parse_batch_res_data(res)
               
            t_after = time.time()
            getLogger(STDOUT).info('SF %s Took time: %.2f' \
                                   % (opname, t_after-t_before))
        if NEW_IDS_TABLE in job_syntax_item:
            results_file_name = \
                 SqlExecutor.csv_name(job_syntax_item[NEW_IDS_TABLE])
            with open(results_file_name, 'w') as result_ids_file:
                csv_data = csv_from_bulk_data(result_ids)
                result_ids_file.write(csv_data)
            getLogger(LOG).info('Saved result ids: %s',
                                     results_file_name)
        getLogger(LOG).info('Done: %s operation', opname)

    def handle_transmitter_merge(self, job_syntax_item, endpoint):
        opname = job_syntax_item[OP_KEY]
        csv_key_val = var_replaced(self.variables, job_syntax_item, CSV_KEY)        
        csv_filename = SqlExecutor.csv_name(csv_key_val)
        csv_data = self.csvdata(csv_filename)
        num_lines = len(csv_data)
        # do nothing for empty data set
        if num_lines <= 1:
            getLogger(LOG).info('skip empty csv')
            from mriya.sf_merge_wrapper import HEADER
            result_ids = BulkData(HEADER, [])
        else:
            objname = job_syntax_item[endpoint]
            conn = self.endpoints.endpoint(endpoint)
            max_batch_size = int(job_syntax_item[BATCH_SIZE_KEY])
            getLogger(STDOUT).info('EXECUTE: %s %s, lines count=%d',
                                     opname, objname, num_lines-1)
            t_before = time.time()
            if len(csv_data):

                result_ids = conn.soap_merge(objname, csv_data, max_batch_size)
            t_after = time.time()
            getLogger(STDOUT).info('SF %s Took time: %.2f' \
                                   % (opname, t_after-t_before))
        if NEW_IDS_TABLE in job_syntax_item:
            results_file_name = \
                 SqlExecutor.csv_name(job_syntax_item[NEW_IDS_TABLE])
            with open(results_file_name, 'w') as result_ids_file:
                csv_data = csv_from_bulk_data(result_ids)                
                result_ids_file.write(csv_data)
            getLogger(LOG).info('Saved result ids: %s',
                                     results_file_name)
        getLogger(LOG).info('Done: %s operation', opname)
        
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
            elif opname == OP_MERGE:
                self.handle_transmitter_merge(job_syntax_item, endpoint)


