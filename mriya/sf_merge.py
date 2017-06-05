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
import requests

from sfbulk.exceptions import BulkException
from sfbulk.utils_xml import parseXMLResult


# XML CONSTANS

MERGE_SOAP_REQUEST_HEADERS = {
    u'content-type': 'text/xml',
    u'charset': 'UTF-8',
    u'SOAPAction': ''
}

#params: sessionid, mergerequest
# see MERGE_REQUEST_BODY_PART for mergerequest
MERGE_SOAP_REQUEST_BODY = u"""<?xml version="1.0" encoding="utf-8" ?>
<soapenv:Envelope 
    xmlns:soapenv="<a rel="nofollow" class="external free" href="http://schemas.xmlsoap.org/soap/envelope/">http://schemas.xmlsoap.org/soap/envelope/</a>"
    xmlns:urn="urn:enterprise.soap.sforce.com"
    xmlns:urn1="sobject.enterprise.soap.sforce.com"
    xmlns:xsi="<a rel="nofollow" class="external free" href="http://www.w3.org/2001/XMLSchema-instance">http://www.w3.org/2001/XMLSchema-instance</a>">
    <soapenv:Header>
        <urn:SessionHeader>
            <urn:sessionId><b>{sessionid}</b></urn:sessionId>
         </urn:SessionHeader>
    </soapenv:Header>
    <soapenv:Body>
        <urn:merge>
          {mergerequest}
        </urn:merge>
    </soapenv:Body>
</soapenv:Envelope>"""


#params: objname, masterid, mergeidslist
# see MERGE_IDS_LIST_BODY_PART for mergeidslist
MERGE_REQUEST_BODY_PART = u"""
            <urn:request>
                <urn:masterRecord xsi:type="urn1:<b>{objname}</b>">
                    <urn1:Id><b>{masterid}</b></urn1:Id>
                        <Description><b>Merged with Dupe {objname}.</b></Description>
                </urn:masterRecord>
                <urn:recordToMergeIds>
                    {mergeidslist}
                </urn:recordToMergeIds>
            </urn:request>"""

# params: mergeid
MERGE_IDS_LIST_BODY_PART = u"""<b>{mergeid}</b>"""

class SoapMerge(object):
    """Standard enterprise objects merge"""

    # SOAP CONSTANS

    SOAP_URL = u'https://{instance_url}/services/Soap/c/{sf_version}'
    DOMAIN = u''

    def __init__(self, instance_url, sessionid, version='37.0'):
        """
        @instance_url - hostname
        @sessionid - Id of authenticated session, must be taken from bulk transaport"""

        self.sessionid = sessionid
        self.soap_url = \
            self.SOAP_URL.format(instance_url=instance_url, sf_version=version)
    
    def merge(self, objname, merge_dict):
        """
        Invokes standard object's merge.
        @merge_dict - {'master_record': ['mergeid1', .. , 'mergeidn']}
        """

        # prepare request body
        mergerequest = ''
        for masterid, mergeids in merge_dict.iteritems():
            mergeids = ''
            for mergeid in mergeids:
                mergeids += MERGE_IDS_LIST_BODY_PART.format(mergeid=mergeid)
            mergerequest += MERGE_REQUEST_BODY_PART.format(
                objname=objname, masterid=masterid, mergeidslist=mergeids)

        merge_soap_request_body = MERGE_SOAP_REQUEST_BODY.format(
            sessionid=self.sessionid,
            mergerequest=mergerequest)

        print merge_soap_request_body
        response = self._send_merge_request(self.soap_url, merge_soap_request_body)
        self._check_response(response)
        return self._get_result(response)
        
    # HELPERS

    def _get_result(self, response):
        dict_result = parseXMLResult(response.content)
        

    def _send_merge_request(self, soap_url, merge_soap_request_body):
        return requests.post(soap_url,
                             merge_soap_request_body,
                             headers=MERGE_SOAP_REQUEST_HEADERS)
        
    @staticmethod
    def _check_response(response):
        if response.status_code != 200:
            dict_result = parseXMLResult(response.content)
            except_msg = dict_result['sf:exceptionMessage']
            except_code = dict_result['sf:exceptionCode']
            raise BulkException('{message}: {code}'.format(
                message=except_msg, code=except_code))
