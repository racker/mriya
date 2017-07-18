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

import time
import logging
from StringIO import StringIO
from logging import getLogger
from sets import Set
from mriya.sql_executor import SqlExecutor, datadir
from mriya.job_syntax import JobSyntax
from mriya.job_syntax import CSVLIST_KEY, QUERY_KEY, CSV_KEY, VAR_KEY
from mriya.job_syntax import BATCH_BEGIN_KEY, BATCH_PARAMS_KEY
from mriya.job_syntax import CONST_KEY, DST_KEY, SRC_KEY
from mriya.opexecutor import Executor
from mriya.bulk_data import get_bulk_data_from_csv_stream
from mriya.log import loginit, ismoreinfo, STDOUT, LOG, MOREINFO

SQLITE_SCRIPT_FMT='.mode csv\n\
.separator ","\n\
.nullvalue "#N/A"\n\
{imports}\n\
{output}\n\
{query}'


def observer(refname, retcode, output):
    if output:
        return (retcode, output.read())

class SqliteExecutor(SqlExecutor):
    table_columns = {}

    def __init__(self, job_syntax_item, variables):
        super(SqliteExecutor, self).__init__(job_syntax_item, variables)
        #loginit(__name__)
        self.query = None
        self.query = self.get_query()

    def get_query(self):
        if not self.query:
            self.query = SqlExecutor.prepare_query_put_vars(
                self.job_syntax_item[QUERY_KEY],
                self.variables)
            # get csvlist after variables substitution
            values = JobSyntax.parse_query_params(self.get_query(), {})
            if CSVLIST_KEY in values:
                getLogger(LOG).debug('Fix csvlist: %s',
                                         values[CSVLIST_KEY])
                if CSVLIST_KEY in self.job_syntax_item:
                    tmp = self.job_syntax_item[CSVLIST_KEY]
                    tmp.extend(values[CSVLIST_KEY])
                    # filter out duplicate items
                    self.job_syntax_item[CSVLIST_KEY] = list(Set(tmp))
                else:
                    self.job_syntax_item[CSVLIST_KEY] = values[CSVLIST_KEY]
    
            if not CONST_KEY in self.job_syntax_item:
                self.query = self.query.replace(CSV_KEY+'.', '')
                self.query = self.query.replace(DST_KEY+'.', '')
                self.query = self.query.replace(SRC_KEY+'.', '')
    
            # end query with ';' add if ';' not exist
            if self.query and self.query[-1] != ';':
                self.query += ';'
        return self.query

    def _create_script(self, variables):
        imports = ''
        if CSVLIST_KEY in self.job_syntax_item :
            for table_name in self.job_syntax_item[CSVLIST_KEY] :
                #check if table name contain vars
                table_name = SqlExecutor.prepare_query_put_vars(
                    table_name, self.variables)
                imports += ".import {csv} {name}\n"\
                    .format(csv=self.csv_name(table_name), name=table_name)
        output = ''
        if CSV_KEY in self.job_syntax_item:
            table_name = self.job_syntax_item[CSV_KEY]
            output += ".headers on\n"
            output += ".output {csv}\n"\
                .format(csv=self.csv_name(table_name))
            getLogger(LOG).info('working on table=%s', table_name)
        elif VAR_KEY in self.job_syntax_item:
            output += ".headers off\n"
            output += ".output stdout\n"
        elif BATCH_BEGIN_KEY in self.job_syntax_item:
            output += ".headers on\n"
            output += ".output stdout\n"
        getLogger(MOREINFO).info('EXECUTE [CSV]: %s', self.get_query())
        input_data = SQLITE_SCRIPT_FMT.format(imports=imports,
                                              output=output,
                                              query=self.get_query())
        return input_data

    def fix_empty_res_table(self, table_name):
        size = SqlExecutor.csv_size(table_name)
        if size == 0:
            getLogger(LOG).debug("Fix empty csv for table %s",
                                     table_name)
            cols = SqlExecutor.get_query_columns(self.get_query())
            header = ','.join(cols)+'\n'
            with open(self.csv_name(table_name), 'w') as f:
                f.write(header)

    def execute(self):
        t_before = time.time()
        executor = Executor()
        cmd = 'sqlite3 -batch'
        script = self._create_script(self.variables)
        getLogger(LOG).debug('Sqlite script:\n%s', script)
        executor.execute('refname', cmd,
                         input_data=script,
                         output_pipe=True)
        res = executor.poll_for_complete(observer)
        del executor
        t_after = time.time()
        csvname = ''
        if CSV_KEY in self.job_syntax_item:
            csvname = self.job_syntax_item[CSV_KEY]
            self.fix_empty_res_table(csvname)
            if ismoreinfo():
                getLogger(MOREINFO).info('%s.csv - %.2fs' % (csvname, t_after-t_before))
            else:
                getLogger(STDOUT).info('.')
        res = res['refname']
        if res[0] != 0:
            raise Exception("Sqlite query error", self.get_query())
        else:
            self._handle_var_create(res)
            retcode = res[1]
            return retcode

    def _handle_var_create(self, res):
        if VAR_KEY in self.job_syntax_item:
            self.save_var(self.job_syntax_item[VAR_KEY], res[1].strip())
        elif BATCH_BEGIN_KEY in self.job_syntax_item:
            param_field_name = self.job_syntax_item[BATCH_BEGIN_KEY][0]
            stream = StringIO(res[1])
            csv_batch_params =  get_bulk_data_from_csv_stream(stream)
            if csv_batch_params.fields:
                field_idx = csv_batch_params.fields.index(param_field_name)
                values = [x[field_idx] for x in csv_batch_params.rows]
                self.variables[BATCH_PARAMS_KEY] = values
            else:
                # no batches to process
                self.variables[BATCH_PARAMS_KEY] = []
