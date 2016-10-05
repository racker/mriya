#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from StringIO import StringIO
from mriya.job_syntax import JobSyntax
from mriya.job_syntax import SQL_TYPE_SQLITE, SQL_TYPE_SF
from mriya.sqlite_executor import SqliteExecutor

def create_executor(job_syntax_item, variables):
    sql_exec = None
    sqltype = JobSyntax.sqltype(job_syntax_item)
    if sqltype == SQL_TYPE_SQLITE:
        sql_exec = SqliteExecutor(job_syntax_item, variables)
    elif sqltype == SQL_TYPE_SF:
        sql_exec = None
    return sql_exec


def test_read():
    text = 'SELECT \\\n\
1 => csv:const1\n\
SELECT 1 => var:MIN => dst:foo'
    test_stream = StringIO(text)
    lines = JobSyntax.prepare_lines(test_stream.readlines())
    assert lines == ['SELECT 1 => csv:const1',
                     'SELECT 1 => var:MIN => dst:foo']

def test_parse():
    lines = ['SELECT 1 => csv:const1',
             'SELECT 1 => var:MIN => dst:foo => op:insert',
             'SELECT f1, (SELECT f2 FROM csv.one_ten) as f10 FROM csv.one_ten, 9; => csv:final']
    exp_values1 = {'query': 'SELECT 1',
                   'csv': 'const1'}
    res_values1 = JobSyntax.parse_line(lines[0])
    print res_values1
    assert res_values1 == exp_values1
    exp_values2 = {'query': 'SELECT 1',
                   'var': 'MIN',
                   'dst': 'foo',
                   'op': 'insert'}
    res_values2 = JobSyntax.parse_line(lines[1])
    print res_values2
    assert res_values2 == exp_values2
    exp_values3 = {'query': 'SELECT f1, (SELECT f2 FROM one_ten) as f10 FROM one_ten, 9;',
                   'csv': 'final',
                   'from': 'csv',
                   'csvlist': ['one_ten']}
    res_values3 = JobSyntax.parse_line(lines[2])
    print res_values3
    assert res_values3 == exp_values3

def test_var_csv():
    variables = {}
    lines = ['SELECT 1; => var:one',
             'SELECT 9; => var:nine',
             'SELECT {one} as f1, {nine}+1 as f2; => csv:one_ten',
             'SELECT f1, {nine} as f9, (SELECT f2 FROM csv.one_ten) as f10 FROM csv.one_ten; => csv:one_nine_ten']
    job_syntax = JobSyntax(lines)
    sql_exec = create_executor(job_syntax[0], variables)
    sql_exec.execute()
    variables = sql_exec.variables
    del sql_exec

    sql_exec = create_executor(job_syntax[1], variables)
    sql_exec.execute()
    variables = sql_exec.variables
    del sql_exec

    sql_exec = create_executor(job_syntax[2], variables)
    sql_exec.execute()
    variables = sql_exec.variables
    del sql_exec

    sql_exec = create_executor(job_syntax[3], variables)
    sql_exec.execute()
    variables = sql_exec.variables
    del sql_exec

    print variables

if __name__ == '__main__':
    test_read()
    test_parse()
    test_var_csv()
