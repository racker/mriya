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
from mriya.sf_merge import SoapMerge, MergeData
from logging import getLogger
from mriya.log import loginit, STDERR, STDOUT, LOG
from mriya.bulk_data import BulkData

#Resolve the "Too many SOQL queries: 101" error
#To fix the issue, change your code so that the number of SOQL fired is less than 100.
MAX_CHUNKS_COUNT = 99
HEADER = ("Id","Success","StatusCode","Message")
HEADER_CSV = ['"Id","Success","StatusCode","Message"\n']

class SfSoapMergeWrapper(object):
    """ This object is intended to be created just before merge operation, 
    and to be deleted after operation completion. One instance for one merge operation. """

    def __init__(self, sf_bulk_connector, objname, bulk_data, max_chunk_size):
        self.sf_bulk_connector = sf_bulk_connector
        self.objname = objname
        self.bulk_data = bulk_data
        self.max_chunk_size = max_chunk_size

    def sessionid(self):
        return self.sf_bulk_connector.bulk.sessionid

    def instance_url(self):
        return self.sf_bulk_connector.instance_url
    
    def validate(self):
        """ should be executed before run_merge """
        supported = ['lead', 'contact', 'account']
        if self.objname.lower() not in supported :
            msg = 'Merge Error: The only supported object types are %s' % (supported)
            getLogger(STDERR).error(msg)
            return None
        self.merge_data = self._bulkdata_to_mergedict(self.bulk_data)
        return self.merge_data
   
    def run_merge(self):
        merger = SoapMerge(self.instance_url(), self.sessionid())
        if self.merge_data:
            rows = []
            current_chunk = {}
            for k,v in self.merge_data.iteritems():
                current_chunk[k] = v
		if len(current_chunk) == self.max_chunk_size:
                    res = merger.merge(self.objname, current_chunk)
                    rows.extend(res)
                    current_chunk.clear()
            res = merger.merge(self.objname, current_chunk)
            rows.extend(res)
            bulk_data = BulkData(fields=HEADER, rows = rows)
        else:
            bulk_data = BulkData(fields=HEADER, rows = [])
        return bulk_data

    #HELPERS:

    @staticmethod    
    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in xrange(0, len(l), n):
            yield l[i:i + n]
    
    @staticmethod
    def isvalidid(iddata):
        err = 0
        try:
            iddata.decode('ascii')
        except UnicodeDecodeError:
            err = 1
        if (len(iddata) == 15 or len(iddata) == 18) and not err:
            return True
        return False

    @staticmethod
    def _bulkdata_to_mergedict(bulk_data):
        mergedict = {}
        fields = [x.lower() for x in bulk_data.fields]
        try:
            master_idx = fields.index('masterrecordid')
            merge_idx = fields.index('mergerecordid')
        except ValueError:
            msg = 'Error: MasterRecordId & MergeRecordId columns are required for merge'
            getLogger(STDERR).error(msg)
            return None
        for pair in bulk_data.rows:
            masterrecid = pair[master_idx]
            mergerecid = pair[merge_idx]
            if not (SfSoapMergeWrapper.isvalidid(masterrecid) and \
                    SfSoapMergeWrapper.isvalidid(mergerecid)):
                msg = 'Error: Invalid Salesforce rec ids %s are provided for merge' % (pair)
                getLogger(STDERR).error(msg)
                return None
            if masterrecid not in mergedict:
                mergedict[masterrecid] = []
            mergedict[masterrecid].append(mergerecid)
        for master, dups in mergedict.iteritems():
            if len(dups) > 2:
                getLogger(STDERR).error("Can't merge more than three recs at a time: %s, %s" \
                                        % (master, dups))
                return None
        return mergedict
    
