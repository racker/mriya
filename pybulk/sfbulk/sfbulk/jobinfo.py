"""
Copyright (C) 2012-2013 by Clearcode <http://clearcode.cc>
and associates (see AUTHORS).

This file is part of sfbulk.

sfbulk is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

sfbulk is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with sfbulk.  If not, see <http://www.gnu.org/licenses/>.
"""
import xml.dom.minidom

from sfbulk.exceptions import BulkException
from sfbulk.utils_xml import createxmlNode


class BulkJobInfo(object):
    """
    Class to hold jobs information.
    Every jobs information can holds multiple batches
    each of the batch id are stored in property "batch".
    """

    id = None
    batch = None
    state = None
    debug_result = None
    concurrencyMode = u'Parallel'
    contentType = u'CSV'
    operation = None
    _object = None
    externalfieldname = None
    jobstate = None

    # WORDS
    JOBINFO = u'jobinfo'
    XMLNS = u'xmlns'
    ASYNCAPI = u'http://www.force.com/2009/06/asyncapi/dataload'
    UTF8 = u'utf-8'
    OPERATION = u'operation'
    OBJECT = u'object'
    EXTERNALIDFIELDNAME = u'externalIdFieldName'
    CONCURRENCYMODE = u'concurrencyMode'
    CONTENTTYPE = u'contentType'

    def __init__(self, logger=None):
        self.batch = {}
        self.logger = logger

    def findBatchState(self, batchId):
        """
        Lookup latest batch state which is stored.

        @type: string
        @param batchId: batch id which is inspected for it's batch status
        """
        if batchId in self.batch:
            return self.batch[batchId]['state']
        else:
            message = 'Batch: %s not found' % batchId
            self.logger.exception(message)
            raise BulkException(message)

    def createJob(self):
        """
        Creating individual job information.
        This information will correspondent
        to single job information in Salesforce.
        """
        root, jobinfo = self.__set_headers()

        if self.operation is not None:
            jobinfo.appendChild(
                createxmlNode(self.OPERATION, str(self.operation).lower()))
        if self._object is not None:
            jobinfo.appendChild(createxmlNode(self.OBJECT, self._object))
        if self.externalfieldname is not None:
            jobinfo.appendChild(
                createxmlNode(self.EXTERNALIDFIELDNAME,
                              self.externalfieldname))
        if self.concurrencyMode is not None:
            jobinfo.appendChild(
                createxmlNode(self.CONCURRENCYMODE, self.concurrencyMode))
        if self.contentType is not None:
            jobinfo.appendChild(
                createxmlNode(self.CONTENTTYPE, self.contentType))

        return self.__return_xml(root, jobinfo)

    def closeJob(self):
        """
        Close the individual job information.
        """
        root, jobinfo = self.__set_headers()

        if self.state is not None:
            jobinfo.appendChild(createxmlNode('state', self.state))

        return self.__return_xml(root, jobinfo)

    def __set_headers(self):
        root = xml.dom.minidom.Document()
        jobinfo = root.createElement(self.JOBINFO)
        jobinfo.setAttribute(
            self.XMLNS, self.ASYNCAPI)
        return root, jobinfo

    def __return_xml(self, root, jobinfo):
        root.appendChild(jobinfo)
        return root.toxml(self.UTF8)


class JobInfo(object):
    """
    JobInfo class is preparing jobinfo data.
    """

    @staticmethod
    def factory(operation, sf_object, externalidfield=None):
        """
        Prepares jobinfo object.

        @type operation: type of Salesforce operation
        @param operation: type of operation eg. upsert
        @type _object: type of Salesforce object
        @param _object: type of object eg. Account
        @type _object: type of Salesforce object
        @param _object: type of object eg. Account

        @rtype: JobInfo object
        @return:JobInfo object filled with predefined data
        """

        # JOB OPERATION
        UPSERT = u'upsert'
#delete
#insert
#query
#upsert
#update
#hardDelete

        #TODO: check if type is correct

        jobinfo = BulkJobInfo()
        jobinfo.operation = operation
        jobinfo._object = sf_object

        if operation == UPSERT:
            jobinfo.externalfieldname = externalidfield

        return jobinfo
