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

# only sqlite related
CSVLIST_KEY = 'csvlist'


from logging import getLogger
from mriya.log import loginit

class JobSyntax(object):

    def __init__(self, raw_lines):
        loginit(__name__)
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
            values.append(JobSyntax.parse_line(line))
        return values

    @staticmethod
    def ignore_punctuation(line):
        res = "".join(c for c in line if c not in ('!',
                                                   ',',
                                                   '(',
                                                   ')',
                                                   ';'))
        res.strip()
        return res

    @staticmethod
    def parse_query_params(query, values):
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
        transm_pos = line.find(TRANSMITTER, start_pos)
        while transm_pos != -1:
            if QUERY_KEY not in values:
                query = line[start_pos:transm_pos].strip()
                values = JobSyntax.parse_query_params(query, values)
                query = query.replace(CSV_KEY+'.', '')
                query = query.replace(DST_KEY+'.', '')
                query = query.replace(SRC_KEY+'.', '')
                values[QUERY_KEY] = query
            start_pos = transm_pos + len(TRANSMITTER)
            values = JobSyntax.parse_transmitter_value(values,
                                                       line[start_pos:])
            transm_pos = line.find(TRANSMITTER, start_pos)
        if values:
            values[LINE_KEY] = line
        return values
    
    @staticmethod
    def parse_transmitter_value(values, pair):
        if pair.find(TRANSMITTER) != -1:
            pair = pair[:pair.find(TRANSMITTER)]
        key_value = pair.split(':')
        try:
            key = key_value[0].strip()
            val = key_value[1].strip()
        except:
            getLogger(__name__).error('Error: %s', pair)
            raise
        if key == CSV_KEY or key == VAR_KEY:
            values[key] = val
            if len(key_value) > 2:
                cache_flag = key_value[2].strip()
                if cache_flag == CACHE_KEY:
                    values[CACHE_KEY] = True
        elif key == VAR_KEY:
            values[key] = val
        elif key == DST_KEY or key == SRC_KEY:
            objname_val = key_value[2].strip()
            values[OP_KEY] = val
            values[key] = objname_val
            if val == OP_INSERT and len(key_value) > 3:
                values[NEW_IDS_TABLE] = key_value[3].strip()
        elif key == BATCH_BEGIN_KEY:
            val2 = key_value[2].strip()
            values[key] = (val, val2)
        elif key == BATCH_END_KEY:
            values[key] = val
        elif key == MACRO_KEY:
            # support up to two params in the hardcoded way
            values[key] = val
            if len(key_value) >= 4:
                key2 = key_value[2].strip()
                value2 = key_value[3].strip()
                values[REPLACE_KEY] = {key2: value2}
            if len(key_value) == 6:
                key3 = key_value[4].strip()
                value3 = key_value[5].strip()
                values[REPLACE_KEY][key3] = value3
        else:
            print key, val
            assert(0)
        return values

