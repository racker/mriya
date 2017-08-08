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
__author__ = "Yaroslav Litvinov"

from StringIO import StringIO
from opcsv import CsvWriter
from opcsv import CsvReader
from collections import namedtuple

BulkData = namedtuple('BulkData', ['fields', 'rows'])

def get_stream_from_csv_rows_list(csv_rows_list):
    istream = StringIO()
    # put data into stream for handling
    for csv_row in csv_rows_list:
        istream.write(csv_row)
        if len(csv_row) and csv_row[-1] != '\n':
            istream.write('\n')
    istream.seek(0)
    return istream

def csv_from_bulk_data(bulk_data):
    output = StringIO()
    csv_writer = CsvWriter(output, False)
    txt = ''
    for name in bulk_data.fields:
        if txt:
            txt += ','
        txt += name
    txt += '\n'
    csv_writer.write_csv(bulk_data.rows)
    output.seek(0)
    txt += output.getvalue()
    output.close()
    csv_writer.close()
    return txt

def get_bulk_data_from_csv_stream(istream):
    csv_reader = CsvReader(istream)
    fields = None
    rows = []
    while True:
        rec_tuple = csv_reader.read_record()
        if not rec_tuple:
            break
        if not fields:
            fields = rec_tuple
            rec_tuple = None
            continue
        row = []
        for row_item in rec_tuple:
            row.append(row_item)
        rows.append(tuple(row))
    csv_reader.close()
    return BulkData(fields, rows)


def parse_batch_res_data(csv_rows_list):
    istream = get_stream_from_csv_rows_list(csv_rows_list)
    return get_bulk_data_from_csv_stream(istream)


def prepare_received_sf_data(received_data):
    repls = ('\r\n', '<RN CR>'), ('\n', '<N CR>')
    return reduce(lambda a, kv: a.replace(*kv), repls, received_data)

def prepare_sf_data_to_send(data_to_send):
    # use space in repl for doublequotes to be added by sqlite
    repls = ('<RN CR>', '\r\n'), ('<N CR>', '\n')
    return reduce(lambda a, kv: a.replace(*kv), repls, data_to_send)

def save_escape_csv_lines_as_csv_file(csvfname_w, csv_lines):
    with open(csvfname_w, 'w') as csv_f:
        #join incomplete lines and then write complete line
        line = ''
        for csv_line in csv_lines:
            # just add newline, as salesforce anyway replaces 
            # cr by newline when update
            line += csv_line + '\n'
            cnt = line.count('"')
            if not cnt % 2:
                csv_f.write(
                    prepare_received_sf_data(line[:-1])+'\n')
                line = ''
            else:
                continue
        csv_f.flush()
