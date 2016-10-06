#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from mriya.sql_executor import SqlExecutor
from mriya.job_syntax import QUERY_KEY, OBJNAME_KEY, CSV_KEY
from logging import getLogger

class SalesforceExecutor(SqlExecutor):
    def __init__(self, conn, job_syntax_item, variables):
        super(SalesforceExecutor, self).__init__(job_syntax_item, variables)
        self.conn = conn

    def execute(self):
        objname = self.job_syntax_item[OBJNAME_KEY]
        soql_query = self.job_syntax_item[QUERY_KEY]
        bulk_res = self.conn.bulk_load(objname, soql_query)
        self.handle_result(bulk_res)
        retcode = 0
        return retcode

    def handle_result(self, bulk_res):
        getLogger(__name__).info(bulk_res)
        if CSV_KEY in self.job_syntax_item:
            with open(self.job_syntax_item[CSV_KEY]+'.csv', 'w') as csv_f:
                if not len(bulk_res[-1]):
                    #ignore last empty results
                    bulk_res = bulk_res[:-1]
                for csv_line in bulk_res:
                    csv_f.write(csv_line)
                    csv_f.write('\n')
                csv_f.flush()
