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

""" Classes for reading/writing csv files supporting null values
need to be splited to chunks.
CsvReader -- csv reader"""

__author__ = "Yaroslav Litvinov"

import os
import csv
from collections import namedtuple

NULLVAL = '\N'
ESCAPECHAR = '\\'
DELIMITER = ','
LINETERMINATOR = '\n'
DOUBLEQUOTE = True
QUOTING = csv.QUOTE_ALL

def ensure_dir_empty(dirpath):
    """ remove files from dir """
    if not os.path.exists(dirpath):
        os.mkdir(dirpath)
    for fname in os.listdir(dirpath):
        fpath = os.path.join(dirpath, fname)
        if os.path.isfile(fpath):
            os.remove(fpath)

CsvInfo = namedtuple('CsvInfo', ['writer', 'filepath', 'name', 'file_counter'])

class CsvWriter(object):
    """ Csv files writer.
    Supports multiline text values and NULLs, using specific csv format."""
    def __init__(self, output_file, psql_copy, null_val=NULLVAL):
        """ constructor
        output_file -- opened file to write
        psql_copy -- For csv format supported by PSQL_COPY set 'True'.
        null_val -- char in csv file to distguish empty and null values"""
        self.null_val = null_val
        self.psql_copy = psql_copy
        self.file = output_file
        self.csvwriter = csv.writer(output_file,
                                    escapechar=ESCAPECHAR,
                                    delimiter=DELIMITER,
                                    lineterminator=LINETERMINATOR,
                                    doublequote=DOUBLEQUOTE,
                                    quoting=QUOTING)

    def close(self):
        """ close output file"""
        if self.file:
            self.file.close()
            self.file = None

    def write_csv(self, rows):
        """ write list of rows into csv file """
        for row in rows:
            self.csvwriter.writerow(row)
        return len(rows)


################

class CsvReader(object):
    """ Csv files reader, during read it's doing decode, escape of data.
    Supports multiline text values and NULLs, using specific csv format."""
    def __init__(self, input_file, null_val=NULLVAL):
        """ constructor
        input_file -- file opened for reading
        null_val -- char in csv file to distguish empty and null values"""
        self.null_val = null_val
        self.file = input_file
        self.csvreader = csv.reader(input_file,
                                    escapechar=ESCAPECHAR,
                                    delimiter=DELIMITER,
                                    lineterminator=LINETERMINATOR,
                                    doublequote=DOUBLEQUOTE,
                                    quoting=QUOTING)

    def close(self):
        """ close input file """
        self.file.close()
        self.file = None

    def read_record(self):
        """ Return tuple corresponding to single record from csv file """
        def decode_val(val):
            """ return decoded, escaped unicode value """
            if type(val) is str or type(val) is unicode:
                if val == self.null_val:
                    return None
                else:
                    return val.decode('utf-8').decode('unicode-escape')
            else:
                return val

        def prepare_csv_data(row):
            """ return tuple of values, apply decode_val func to every value
            row -- single row from csv"""
            csvvals = []
            for val in row:
                csvvals.append(decode_val(val))
            return csvvals

        try:
            return prepare_csv_data(self.csvreader.next())
        except StopIteration:
            return None
