#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import logging
from StringIO import StringIO
from logging import getLogger
from mriya.sql_executor import SqlExecutor
from mriya.job_syntax import CSVLIST_KEY, QUERY_KEY, CSV_KEY, VAR_KEY
from mriya.job_syntax import BATCH_BEGIN_KEY, BATCH_PARAMS_KEY
from mriya.opexecutor import Executor
from mriya.bulk_data import get_bulk_data_from_csv_stream
from mriya.log import loginit

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
    def __init__(self, job_syntax_item, variables):
        super(SqliteExecutor, self).__init__(job_syntax_item, variables)
        loginit(__name__)

    def _create_script(self, variables):
        imports = ''
        if CSVLIST_KEY in self.job_syntax_item :
            for csv_name in self.job_syntax_item[CSVLIST_KEY] :
                imports += ".import {name}.csv {name}\n"\
                    .format(name=csv_name)
        output = ''
        if CSV_KEY in self.job_syntax_item:
            csvfile = self.job_syntax_item[CSV_KEY]
            output += ".headers on\n"
            output += ".output {csvfile}.csv\n".format(csvfile=csvfile)
        elif VAR_KEY in self.job_syntax_item:
            output += ".headers off\n"
            output += ".output stdout\n"
        elif BATCH_BEGIN_KEY in self.job_syntax_item:
            output += ".headers on\n"
            output += ".output stdout\n"
        query = self.job_syntax_item[QUERY_KEY]
        for var_name, var_value in variables.iteritems():
            if type(var_value) != list:
                query = query.replace('{%s}' % (var_name), var_value)
        input_data = SQLITE_SCRIPT_FMT.format(imports=imports,
                                              output=output,
                                              query=query)
        return input_data

    def execute(self):
        executor = Executor()
        cmd = 'sqlite3 -batch'
        script = self._create_script(self.variables)
        getLogger(__name__).info('Sqlite script:\n%s', script)
        executor.execute('refname', cmd,
                         input_data=script,
                         output_pipe=True)
        res = executor.poll_for_complete(observer)
        del executor
        res = res['refname']
        if res[0] != 0:
            raise Exception("Sqlite query error",
                            self.job_syntax_item[QUERY_KEY])
        else:
            self._handle_var_create(res)
            retcode = res[1]
            return retcode

    def _handle_var_create(self, res):
        if VAR_KEY in self.job_syntax_item:
            self.variables[self.job_syntax_item[VAR_KEY]] = res[1].strip()
        elif BATCH_BEGIN_KEY in self.job_syntax_item:
            param_field_name = self.job_syntax_item[BATCH_BEGIN_KEY][0]
            stream = StringIO(res[1])
            csv_batch_params =  get_bulk_data_from_csv_stream(stream)
            field_idx = csv_batch_params.fields.index(param_field_name)
            values = [x[field_idx] for x in csv_batch_params.rows]
            self.variables[BATCH_PARAMS_KEY] = values
