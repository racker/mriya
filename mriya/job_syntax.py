#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

SQL_TYPE_SQLITE = 'sqlite'
SQL_TYPE_SF = 'salesforce'

TRANSMITTER = '=>'
VAR_KEY = 'var'
CSV_KEY = 'csv'
DST_KEY = 'dst'
SRC_KEY = 'src'
FROM_KEY = 'from' # values 'dst' \ 'src' \ 'csv'
QUERY_KEY = 'query'
LINE_KEY = 'line'
MACRO_KEY = 'macro'
CONST_KEY = 'const'
PUBLISH_KEY = 'publish'

BATCH_PARAMS_KEY = 'batch_params'
BATCH_BEGIN_KEY = 'batch_begin'
BATCH_END_KEY = 'batch_end'

#only salesforce related
OP_KEY = 'op' # values 'insert' \ 'update'
OP_INSERT = 'insert'
OP_UPSERT = 'upsert'
OP_UPDATE = 'update'
OP_DELETE = 'delete'
OBJNAME_KEY = 'objname'
NEW_IDS_TABLE = 'new_ids_table'
CACHE_KEY = 'cache'
REPLACE_KEY = 'replace'
BATCH_SIZE_KEY = 'batch_size'

# only sqlite related
CSVLIST_KEY = 'csvlist'

from logging import getLogger
from itertools import izip
from mriya.log import loginit, STDOUT, LOG

class JobSyntax(object):

    def __init__(self, raw_lines):
        #loginit(__name__)
        raw_lines = JobSyntax.prepare_lines(raw_lines)
        self.values = self.parse_lines(raw_lines)

    def __iter__(self):
        for lst in self.values:
            yield lst

    def __getitem__(self, idx):
        return self.values[idx]

    def items(self):
        return self.values

    @staticmethod
    def sqltype(job_syntax_item):
        if FROM_KEY in job_syntax_item:
            if DST_KEY == job_syntax_item[FROM_KEY] or \
               SRC_KEY == job_syntax_item[FROM_KEY]:
                sqltype = SQL_TYPE_SF
            else:
                sqltype = SQL_TYPE_SQLITE
        else:
            sqltype = SQL_TYPE_SQLITE
        return sqltype

    @staticmethod
    def prepare_lines(lines):
        prepared = []
        current_line = ''
        for line in lines:
            strip_line = line.strip()
            if strip_line and strip_line[-1] == '\\':
                current_line += strip_line[:-1]
            else:
                if current_line:
                    current_line += strip_line
                    prepared.append(current_line)
                    current_line = ''
                else:
                    prepared.append(strip_line)
        return prepared

    @staticmethod
    def parse_lines(lines):
        values = []
        for line in lines:
            job_syntax_item = JobSyntax.parse_line(line)
            if LINE_KEY in job_syntax_item and job_syntax_item[LINE_KEY]:
                values.append(job_syntax_item)
        return values

    @staticmethod
    def ignore_punctuation(line):
        res = "".join(c for c in line if c not in ('!',
                                                   ',',
                                                   '(',
                                                   ')',
                                                   ';',
                                                   '\'',
                                                   '"'))
        res.strip()
        return res

    @staticmethod
    def parse_query_params(query, values):
        query = query.replace(';', ' ')
        while True:
            src_pos = query.find(SRC_KEY + '.')
            dst_pos = query.find(DST_KEY + '.')
            csv_pos = query.find(CSV_KEY + '.')
            if dst_pos != -1:
                # external sf query
                values[FROM_KEY] = DST_KEY
                objname_pos = dst_pos + len(DST_KEY) + 1
                values[OBJNAME_KEY] = query[objname_pos:].split(' ')[0]
                break
            elif src_pos != -1:
                # external sf query
                values[FROM_KEY] = SRC_KEY
                objname_pos = src_pos + len(SRC_KEY) + 1
                values[OBJNAME_KEY] = query[objname_pos:].split(' ')[0]
                break
            elif csv_pos != -1:
                # external sqlite csv
                values[FROM_KEY] = CSV_KEY
                if CSVLIST_KEY not in values:
                    values[CSVLIST_KEY] = []
                query = query[csv_pos+len(CSV_KEY):]
                draft_csv_name = query.split('.')[1]
                csv_name = JobSyntax.ignore_punctuation(draft_csv_name)
                csv_name = csv_name.split()[0]
                if csv_name not in values[CSVLIST_KEY]:
                    values[CSVLIST_KEY].append(csv_name)
            else:
                break
        return values
    
    @staticmethod
    def parse_line(line):
        values = {}
        if line.lstrip().find('--') == 0:
            return values
        start_pos = 0
        start_query_pos = None
        end_query_pos = None
        transm_pos = line.find(TRANSMITTER, start_pos)
        while transm_pos != -1:
            if start_query_pos is None:
                start_query_pos = start_pos
            if end_query_pos is None:
                end_query_pos = transm_pos
            start_pos = transm_pos + len(TRANSMITTER)
            values = JobSyntax.parse_transmitter_value(
                values, line[start_pos:])
            transm_pos = line.find(TRANSMITTER, start_pos)
        query = line[start_query_pos:end_query_pos].strip()
        values = JobSyntax.parse_query_params(query, values)
        if not CONST_KEY in values:
            query = query.replace(CSV_KEY+'.', '')
            query = query.replace(DST_KEY+'.', '')
            query = query.replace(SRC_KEY+'.', '')
        values[LINE_KEY] = line
        values[QUERY_KEY] = query
        return values
    
    @staticmethod
    def parse_transmitter_value(values, pair):
        if pair.find(TRANSMITTER) != -1:
            pair = pair[:pair.find(TRANSMITTER)]
        key_vals = [x.strip() for x in pair.split(':')]
        try:
            key = key_vals[0]
            val = key_vals[1]
        except:
            getLogger(STDOUT).error('Error parsing transmitter value: %s', pair)
            raise
        if key == CSV_KEY:
            values[key] = val
            if len(key_vals) > 2:
                cache_flag = key_vals[2]
                if cache_flag == CACHE_KEY:
                    values[CACHE_KEY] = ''
        elif key == VAR_KEY:
            values[key] = val
            if len(key_vals) > 2:
                flag = key_vals[2]
                if flag == PUBLISH_KEY:
                    values[PUBLISH_KEY] = ''
        elif key == CONST_KEY:
            values[key] = val
        elif key == DST_KEY or key == SRC_KEY:
            objname_val = key_vals[2]
            values[OP_KEY] = val
            values[key] = objname_val
            if (val == OP_INSERT or val == OP_UPSERT or \
                val == OP_DELETE or val == OP_UPDATE):
                if len(key_vals) >= 5:
                    values[BATCH_SIZE_KEY] = key_vals[3]
                    values[NEW_IDS_TABLE] = key_vals[4]
                else:
                    getLogger(LOG).info("%d,%s", len(key_vals), pair)
                    getLogger(STDOUT).error("Batch parameters required")
                    assert(0)
        elif key == BATCH_BEGIN_KEY:
            val2 = key_vals[2]
            values[key] = (val, val2)
        elif key == BATCH_END_KEY:
            values[key] = val
        elif key == MACRO_KEY:
            values[key] = val
            # count of rest params must be even
            assert not len(key_vals) % 2
            it = iter(key_vals[2:])
            for key, val in izip(it, it):
                if REPLACE_KEY not in values:
                    values[REPLACE_KEY] = {key: val}
                else:
                    values[REPLACE_KEY][key] = val
        else:
            print key, val
            assert(0)
        return values

