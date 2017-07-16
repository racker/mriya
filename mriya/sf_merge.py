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
from collections import namedtuple
from sfbulk.utils_xml import parseXMLResultList

MergeData = namedtuple('MergeData', ['MasterRecordId', 'MergeRecordId'] )

# XML CONSTANS

RESPONSE_LIST_NAME = 'mergeResponse'

MERGE_SOAP_REQUEST_HEADERS = {
    u'content-type': 'text/xml',
    u'charset': 'UTF-8',
    u'SOAPAction': 'merge'
}

#params: sessionid, mergerequest
# see MERGE_REQUEST_BODY_PART for mergerequest
MERGE_SOAP_REQUEST_BODY = u"""<?xml version="1.0" encoding="utf-8" ?>
<soapenv:Envelope 
    xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
    xmlns:urn="urn:enterprise.soap.sforce.com"
    xmlns:urn1="sobject.enterprise.soap.sforce.com"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <soapenv:Header>
        <urn:SessionHeader>
            <urn:sessionId>{sessionid}</urn:sessionId>
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
MERGE_REQUEST_BODY_PART = u"""            <urn:request>
                <urn:masterRecord xsi:type="urn1:{objname}">
                    <urn1:Id>{masterid}</urn1:Id>
                </urn:masterRecord>
                    {mergeidslist}
            </urn:request>"""

# params: mergeid
MERGE_IDS_LIST_BODY_PART = u"<urn:recordToMergeIds>{mergeid}</urn:recordToMergeIds>"

class SoapException(Exception):

    default_detail = u'Soap Exception'

    def __init__(self, detail=u''):
        """
        Soap Exception class.
        """
        super(SoapException, self).__init__()
        if detail:
            self.detail = detail

    def get_detail(self):
        return self.detail or self.default_detail

    def __str__(self):
        return self.get_detail()


class SoapMerge(object):
    """Standard enterprise objects merge"""

    # SOAP CONSTANS

    SOAP_URL = u'{instance_url}/services/Soap/c/{sf_version}'

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
        keys_to_save_order = []
        
        # prepare request body
        mergerequest = ''
        for masterid, mergeidsl in merge_dict.iteritems():
            keys_to_save_order.append(masterid)
            mergeids = ''
            for mergeid in mergeidsl:
                mergeids += MERGE_IDS_LIST_BODY_PART.format(mergeid=mergeid)
            mergerequest += MERGE_REQUEST_BODY_PART.format(
                objname=objname, masterid=masterid, mergeidslist=mergeids)

        merge_soap_request_body = MERGE_SOAP_REQUEST_BODY.format(
            sessionid=self.sessionid,
            mergerequest=mergerequest)

        response = self._send_merge_request(self.soap_url, merge_soap_request_body)
        self._check_response(response)
        rows_res = self._parse_merge_results(
            self._result(
                parseXMLResultList(response.content, RESPONSE_LIST_NAME)))
        return self._get_ordered_results(keys_to_save_order, rows_res)
        return res
        
    # HELPERS

    @staticmethod
    def _get_ordered_results(ordered_keys, rows_res):
        # As some of sf results not returning id in result, add it here
        ordered_rows = []
        for idx, key in enumerate(ordered_keys):
            row = rows_res[idx]
            id_val = row[0]
            if id_val and key != id_val:
                raise Exception('Soap Merge internal error')
            else:
                row[0] = key
            ordered_rows.append(row)
        return ordered_rows
    
    @staticmethod
    def _parse_merge_results(results):
        res = []
        for dict_res in results:
            oneparsed = []
            if type(dict_res) is dict:
                if 'id' in dict_res:
                    oneparsed.append(dict_res['id'])
                else:
                    oneparsed.append('')
                oneparsed.append(dict_res['success'])
                if 'statusCode' in dict_res:
                    oneparsed.append(dict_res['statusCode'])
                else:
                    oneparsed.append('')
                if 'message' in dict_res:
                    oneparsed.append(dict_res['message'])
                else:
                    oneparsed.append('')
            res.append(oneparsed)
        return res

    @staticmethod
    def _result(res):
        if RESPONSE_LIST_NAME in res:
            return res[RESPONSE_LIST_NAME]
        else:
            return [res]

    def _send_merge_request(self, soap_url, merge_soap_request_body):
        return requests.post(soap_url,
                             merge_soap_request_body,
                             headers=MERGE_SOAP_REQUEST_HEADERS)
        
    def _check_response(self,response):
        res = self._result(parseXMLResultList(response.content, RESPONSE_LIST_NAME))
        if not res:
            raise Exception('Bad soap merge response')
        else:
            dict_res = res[0]
        if response.status_code != 200:
            fault_string = dict_res['faultstring']
            fault_code = dict_res['faultcode']
            raise SoapException('{message}: {code}'.format(
                message=fault_string, code=fault_code))


        
