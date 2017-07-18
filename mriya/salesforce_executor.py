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
from logging import getLogger
from mriya.sql_executor import SqlExecutor
from mriya.job_syntax import QUERY_KEY, OBJNAME_KEY, CSV_KEY, VAR_KEY
from mriya.job_syntax import CONST_KEY, DST_KEY, SRC_KEY, FROM_KEY
from mriya import bulk_data
from mriya.log import loginit, STDOUT

EMPTY_SF_RESPONSE = 'Records not found for this query'

class SalesforceExecutor(SqlExecutor):
    def __init__(self, conn, job_syntax_item, variables):
        super(SalesforceExecutor, self).__init__(job_syntax_item, variables)
        self.conn = conn
        #loginit(__name__)
        self.query = None
    
    def get_query(self):
        if not self.query:
            self.query = SqlExecutor.prepare_query_put_vars(
                self.job_syntax_item[QUERY_KEY],
                self.variables)
            if not CONST_KEY in self.job_syntax_item:
                self.query = self.query.replace(CSV_KEY+'.', '')
                self.query = self.query.replace(DST_KEY+'.', '')
                self.query = self.query.replace(SRC_KEY+'.', '')
            # get rid of trailing ';' in query
            if self.query and self.query[-1] == ';':
                self.query = self.query[:-1]
        return self.query

    def execute(self):
        objname = self.job_syntax_item[OBJNAME_KEY]
        instname = ''
        if FROM_KEY in self.job_syntax_item:
            instname = self.job_syntax_item[FROM_KEY]
        elif DST_KEY in self.job_syntax_item:
            instname = self.job_syntax_item[DST_KEY]
        elif SRC_KEY in self.job_syntax_item:
            instname = self.job_syntax_item[SRC_KEY]
        t_before = time.time()
        getLogger(STDOUT).info("Execute [%s.%s]: %s",
                                 instname, objname,
                                 self.get_query())
        bulk_res = self.conn.bulk_load(objname, self.get_query())
        self.handle_result(bulk_res)
        t_after = time.time()
        getLogger(STDOUT).info('SF Took time: %.2f' % (t_after-t_before))
        retcode = 0
        return retcode

    def handle_result(self, bulk_res):
        # handle empty result - fix it by adding column names
        if bulk_res and bulk_res[0] == EMPTY_SF_RESPONSE:
            cols = SqlExecutor.get_query_columns(self.get_query())
            header = ','.join(cols)
            bulk_res = [header]

        if len(bulk_res) > 1:
            #ignore last empty results
            bulk_res = bulk_res[:-1]

        # handle result
        if CSV_KEY in self.job_syntax_item:
            csvfname = SqlExecutor.csv_name(self.job_syntax_item[CSV_KEY])
            bulk_data.save_escape_csv_lines_as_csv_file(csvfname, bulk_res)
        elif VAR_KEY in self.job_syntax_item:
            res = bulk_data.parse_batch_res_data(bulk_res)
            if res.rows:
                self.save_var(self.job_syntax_item[VAR_KEY],
                              res.rows[0][0])

