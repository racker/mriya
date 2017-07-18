"""
Copyright (C) 2016-2017 by Yaroslav Litvinov <yaroslav.litvinov@gmail.com>
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
#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"

from sfbulk.utils_xml import parseXMLResult, parseXMLResultList

SOAP_MERGE_RESP = """<?xml version="1.0" encoding="UTF-8"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns="urn:enterprise.soap.sforce.com" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><soapenv:Body><mergeResponse><result><errors><fields xsi:nil="true"/><message>This Company has Accounts associated with it, and cannot be deleted. Please contact a Salesforce Administrator for assistance.</message><statusCode>DELETE_FAILED</statusCode></errors><id>00161000004xxAfAAI</id><success>false</success></result><result><errors><fields xsi:nil="true"/><message>entity is deleted</message><statusCode>ENTITY_IS_DELETED</statusCode></errors><id>0016100000M94ppAAB</id><success>false</success></result></mergeResponse></soapenv:Body></soapenv:Envelope>"""

SOAP_MERGE_PARSED = {
    'mergeResponse': [
        {
            u'message': u'This Company has Accounts associated with it, and cannot be deleted. Please contact a Salesforce Administrator for assistance.',
            u'id': u'00161000004xxAfAAI',
            u'success':
            u'false',
            u'statusCode': u'DELETE_FAILED'},
        {
            u'message': u'entity is deleted',
            u'id': u'0016100000M94ppAAB',
            u'success': u'false',
            u'statusCode': u'ENTITY_IS_DELETED'}
    ]}

def test_empty():
    raw_xml = '<empty></empty>'
    res = parseXMLResult(raw_xml)
    assert res == {}

def test_one_result():
    raw_xml = '<result-list xmlns="http://www.force.com/2009/06/asyncapi/dataload"><result>75238000003axrT</result></result-list>'
    res = parseXMLResult(raw_xml)
    assert res == {u'result': u'75238000003axrT'}

def test_many_results():
    raw_xml = '<result-list xmlns="http://www.force.com/2009/06/asyncapi/dataload"><result>75261000003W0MS</result><result>75261000003W0Mc</result></result-list>'
    res = parseXMLResult(raw_xml)
    assert res == {u'result': u'75261000003W0MS',
                   u'result-002': u'75261000003W0Mc'}

def test_big():
    raw_xml = '<batchInfo>\n\
<state>Completed</state>\n\
<numberRecordsProcessed>50000</numberRecordsProcessed>\n\
<totalProcessingTime>500</totalProcessingTime>\n\
</batchInfo>'
    res = parseXMLResult(raw_xml)
    print res
    assert res == {'state': 'Completed',
                   'numberRecordsProcessed': '50000',
                   'totalProcessingTime': '500'}


def test_soap_merge_resp():
    res = parseXMLResultList(SOAP_MERGE_RESP, 'mergeResponse')
    print res
    assert(res == SOAP_MERGE_PARSED)
    
if __name__ == '__main__':
    test_one_result()
    test_many_results()
    test_big()
    test_soap_merge_resp()
