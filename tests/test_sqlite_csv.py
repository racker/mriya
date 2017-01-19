#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from io import BytesIO
from mriya.opexecutor import Executor
from mriya.bulk_data import get_bulk_data_from_csv_stream

EXPORT_SCRIPT_FMT='create table {name}(id int, field string, field2 string);\n\
insert into {name} values(1, "hi", "there");\n\
insert into {name} values(2, "hello man", "aha");\n\
insert into {name} values(3, "hello, man", "aha");\n\
insert into {name} values(4, "hello, ""man""", "aha");\n\
insert into {name} values(5, "hello\n\
man\n\
hi", "aha");\n\
insert into {name} values(6, "#N/A", "aha");\n\
.headers on\n\
.mode csv\n\
.output stdout\n\
.separator ","\n\
select * from {name};'

IMPORT_SCRIPT_FMT='\n\
{create_table}\n\
.headers on\n\
.mode csv\n\
.output stdout\n\
.separator ","\n\
.import {name}.csv {name}\n\
select * from {name};'

# following csv is compatible with salesforce format
CSV_DATA='id,field,field2\n\
1,hi,there\n\
2,"hello man",aha\n\
3,"hello, man",aha\n\
4,"hello, ""man""",aha\n\
5,"hello\n\
man\n\
hi",aha\n\
6,#N/A,aha\n'

def cmp_csv_lines(csv1, csv2):
    """ Cmp csv after proper parsing instead of blind data coparison.
        This approach solves problem with different csv rows endings
        '\n' vs '\r\n' """
    from StringIO import StringIO
    lines1 = get_bulk_data_from_csv_stream(StringIO(csv1))
    lines2 = get_bulk_data_from_csv_stream(StringIO(csv2))
    return lines1 == lines2

def observer(refname, retcode, output):
    data = None
    if output:
        data = output.read()
    return data

def create_table_get_csv_data(tablename, script):
    executor = Executor()
    cmd = 'sqlite3 -batch'
    executor.execute(tablename, cmd,
                     input_data=script,
                     output_pipe=True)
    res = executor.poll_for_complete(observer)
    del executor
    try:
        assert cmp_csv_lines(res[tablename], CSV_DATA)
        #assert res[tablename] == CSV_DATA
    except:
        print '-------'
        print res[tablename]
        print CSV_DATA
        raise
    return res[tablename]

def test_sqlite_csv_export():
    export_script = EXPORT_SCRIPT_FMT.format(name='test')
    create_table_get_csv_data('test', export_script)

def csv_import_config(tablename, csv_header):
    #fields = csv_header.split(',')
    #fields_wtypes = ['%s string' % i.strip() for i in fields]
    #create_statement = 'create table {name}({fields});'\
    #    .format(name=tablename,
    #            fields=','.join(fields_wtypes))
    return IMPORT_SCRIPT_FMT.format(create_table='',
                                    name=tablename)
    
def test_sqlite_csv_import():
    table_name = 'test2'
    # export csv data
    export_script = EXPORT_SCRIPT_FMT.format(name='foo')
    tmp_data = create_table_get_csv_data('foo', export_script)
    with open(table_name+'.csv', 'w') as csv_file:
        csv_file.write(tmp_data)
    # import csv data
    input_csv_stream = BytesIO(CSV_DATA)
    import_script = csv_import_config(table_name,
                                      input_csv_stream.readline())
    input_csv_stream.close()
    csv_data = create_table_get_csv_data(table_name, import_script)
    assert csv_data == CSV_DATA

if __name__ == '__main__':
    test_sqlite_csv_import()
    test_sqlite_csv_export()
