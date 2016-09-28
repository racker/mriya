#!/usr/bin/env python

""" Classes for reading/writing csv files supporting null values
and multilines.
CsvWriteManager -- wrapper for CsvWriter, support writing to chunked files
CsvWriter -- csv writer, should not be used directly if csv files
need to be splited to chunks.
CsvReader -- csv reader"""

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import os
import csv
from collections import namedtuple

NULLVAL = '\N'
ESCAPECHAR = '\\'
DELIMITER = ','
LINETERMINATOR = '\r\n'
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

class CsvWriteManager(object):
    """ Csv files manager, transparently spliting writing files to files
    with max chunk size during writing."""
    def __init__(self, names, csvs_path, chunk_size):
        """ constructor
        names -- list of base names (psql tables names) of csv files to write
        csvs_path -- folder where to create subfolders for all 'names'
        chunk_size -- max chunk size of file"""
        self.writers = {}
        self.csvs_path = csvs_path
        self.chunk_size = chunk_size
        self.cleandirs(names)

    def cleandirs(self, names):
        """ remove files from dirs list
        names -- names of psql tables corresponding to dirs to erase"""
        for name in names:
            dirpath = os.path.join(self.csvs_path, name)
            ensure_dir_empty(dirpath)

    def create_writer(self, name, fnumber):
        """ return new file writer linked to file chunk with specific number
        name -- psql table name corresponding to csv file
        fnumber -- chunk number of file to create"""
        dirpath = os.path.join(self.csvs_path, name)
        filepath = os.path.join(dirpath, str(fnumber).zfill(5))
        out_f = open(filepath, 'wb')
        wrt = CsvInfo(CsvWriter(out_f, True), filepath, name, fnumber)
        return wrt

    def write_csv(self, name, rows):
        """ Write table records to csv file. Return wrote rows count.
        name -- name of psql
        rows -- rows list to write into csv file related to psql table"""
        if name not in self.writers.keys():
            self.writers[name] = self.create_writer(name, 0)
        elif not self.writers[name].writer.file:
            newfile_count = self.writers[name].file_counter + 1
            self.writers[name] = self.create_writer(name, newfile_count)

        wrt = self.writers[name]
        written_reccount = wrt.writer.write_csv(rows)
        if wrt.writer.file.tell() >= self.chunk_size:
            wrt.writer.close()
        self.writers[name] = wrt
        return written_reccount

    def finalize(self):
        """ close all opened writer objects """
        for name in self.writers:
            self.writers[name].writer.close()

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
