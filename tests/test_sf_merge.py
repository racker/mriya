
import mock
import requests
import requests_mock

from logging import getLogger
from mriya.log import loginit, STDERR, STDOUT, LOG
from mriya.sf_merge import SoapMerge
from mriya.sf_merge_wrapper import SfSoapMergeWrapper, HEADER
from mriya.bulk_data import csv_from_bulk_data, BulkData

MERGE_HTTP_RESP = """<?xml version="1.0" encoding="UTF-8"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns="urn:enterprise.soap.sforce.com"><soapenv:Header><LimitInfoHeader><limitInfo><current>4735</current><limit>5000000</limit><type>API REQUESTS</type></limitInfo></LimitInfoHeader></soapenv:Header><soapenv:Body><mergeResponse><result><errors><message>...</message><statusCode>DELETE_FAILED</statusCode></errors><id>000001111122222711</id><success>false</success></result><result><id>000001111122222789</id><success>true</success></result></mergeResponse></soapenv:Body></soapenv:Envelope>
"""

BULK_DATA_IN = BulkData(fields=('MasterRecordId', 'MergeRecordId'),
                     rows=[('000001111122222789', '000001111122222711'),
                           ('000001111122222789', '000001111122222722')])

BULK_DATA_OUT = BulkData(fields=('Id', 'Success', 'StatusCode', 'Message'),
                         rows=[(u'000001111122222711', u'false', u'DELETE_FAILED', u'...'),
                               (u'000001111122222789', u'true', '', '')])

MERGE_RESP = {
    'mergeResponse' : [
        {
            u'mergedRecordIds': u'0016100000TkTQqAAN',
            u'id': u'0016100000RsjBbAAJ',
            u'success': u'true',
            u'updatedRelatedIds': u'1CA61000001NHFbGAO'},
        {
            u'message': u'entity is deleted',
            u'id': u'0016100000Tl3MFAAZ',
            u'success': u'false',
            u'statusCode': u'ENTITY_IS_DELETED'}
    ]
}

MERGE_PARSED = [
    (u'0016100000RsjBbAAJ', u'true', '', ''),
    (u'0016100000Tl3MFAAZ', u'false', u'ENTITY_IS_DELETED', u'entity is deleted')]


MERGE_CSV_RES = """Id,Success,StatusCode,Message
"0016100000RsjBbAAJ","true","",""
"0016100000Tl3MFAAZ","false","ENTITY_IS_DELETED","entity is deleted"
"""

def test_resp_parser():
    parsed_draft_res = SoapMerge._result(MERGE_RESP)
    res = SfSoapMergeWrapper._parse_merge_results(parsed_draft_res)
    assert res == MERGE_PARSED
    bulk_data = BulkData(fields=HEADER, rows=MERGE_PARSED)
    csv = csv_from_bulk_data(bulk_data)
    print csv
    assert MERGE_CSV_RES == csv    

@requests_mock.Mocker()
def test_mocked_merge_unsupported_object(m):
    loginit(STDERR)
    fake_bulk_connector = mock.Mock()
    fake_bulk_connector.bulk.sessionid = 'fake-sessionid'
    fake_bulk_connector.bulk.instance_url = 'https://fake-localhost'
    
    m.post(url=requests_mock.ANY, text = '')
    sm = SfSoapMergeWrapper(fake_bulk_connector, 'UnsupportedObject', BULK_DATA_IN)
    assert None == sm.validate()
    
@requests_mock.Mocker()
def test_mocked_merge(m):
    loginit(STDERR)
    fake_bulk_connector = mock.Mock()
    m.post(url=requests_mock.ANY, text = MERGE_HTTP_RESP)
    sm = SfSoapMergeWrapper(fake_bulk_connector, 'Account', BULK_DATA_IN)
    sm.sf_bulk_connector.bulk.sessionid = 'fake-sessionid'
    sm.sf_bulk_connector.instance_url = 'https://fake-localhost'
    res = sm.validate()
    assert res != None
    print res
    bulk_data = sm.run_merge()
    print bulk_data
    assert BULK_DATA_OUT == bulk_data
    
if __name__ == '__main__':
    test_resp_parser()
