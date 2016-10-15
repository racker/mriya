#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from logging import getLogger
from mriya.sql_executor import SqlExecutor
from mriya.job_syntax import QUERY_KEY, OBJNAME_KEY, CSV_KEY, VAR_KEY
from mriya.bulk_data import parse_batch_res_data
from mriya.log import loginit

class SalesforceExecutor(SqlExecutor):
    def __init__(self, conn, job_syntax_item, variables):
        super(SalesforceExecutor, self).__init__(job_syntax_item, variables)
        self.conn = conn
        loginit(__name__)
        self.query = None
    
    def get_query(self):
        if not self.query:
            self.query = self.prepare_query_put_vars(
                self.job_syntax_item[QUERY_KEY])
            # get rid of trailing ';' in query
            if self.query and self.query[-1] == ';':
                self.query = self.query[:-1]
        return self.query

    def execute(self):
        objname = self.job_syntax_item[OBJNAME_KEY]
        getLogger(__name__).info("Execute: %s", self.get_query())
        bulk_res = self.conn.bulk_load(objname, self.get_query())
        self.handle_result(bulk_res)
        retcode = 0
        return retcode

    def handle_result(self, bulk_res):
        if CSV_KEY in self.job_syntax_item:
            csvfname = SqlExecutor.csv_name(self.job_syntax_item[CSV_KEY])
            with open(csvfname, 'w') as csv_f:
                if not len(bulk_res[-1]):
                    #ignore last empty results
                    bulk_res = bulk_res[:-1]
                for csv_line in bulk_res:
                    csv_f.write(csv_line)
                    csv_f.write('\n')
                csv_f.flush()
        elif VAR_KEY in self.job_syntax_item:
            res = parse_batch_res_data(bulk_res)
            if res.rows:
                self.save_var(self.job_syntax_item[VAR_KEY],
                              res.rows[0][0])

