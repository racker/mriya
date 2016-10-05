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
CSVLIST_KEY = 'csvlist'
OP_KEY = 'op' # values 'insert' \ 'update'
QUERY_KEY = 'query'


class JobSyntax(object):

    def __init__(self, raw_lines):
        self.values = self.parse_lines(raw_lines)

    def __iter__(self):
        for lst in self.values:
            yield lst

    def __getitem__(self, idx):
        return self.values[idx]

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
            if strip_line[-1] == '\\':
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
                break
            elif src_pos != -1:
                # external sf query
                values[FROM_KEY] = SRC_KEY
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
        if line.find('--') == 0:
            return values
        start_pos = 0
        transm_pos = line.find(TRANSMITTER, start_pos)
        while transm_pos != -1:
            if QUERY_KEY not in values:
                query = line[start_pos:transm_pos].strip()
                values[QUERY_KEY] = query.replace(CSV_KEY+'.', '')
                values = JobSyntax.parse_query_params(query, values)
            start_pos = transm_pos + len(TRANSMITTER)
            values = JobSyntax.parse_transmitter_value(values,
                                                       line[start_pos:])
            transm_pos = line.find(TRANSMITTER, start_pos)
        return values
    
    @staticmethod
    def parse_transmitter_value(values, pair):
        if pair.find(TRANSMITTER) != -1:
            pair = pair[:pair.find(TRANSMITTER)]
        key_value = pair.split(':')
        key = key_value[0].strip()
        val = key_value[1].strip()
        if key == CSV_KEY or key == DST_KEY \
           or key == SRC_KEY or key == VAR_KEY or key == OP_KEY:
            values[key] = val
        else:
            print key, val
            assert(0)
        return values

