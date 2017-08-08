
import mock
import requests
import requests_mock

from logging import getLogger
from mriya.log import loginit, STDERR, STDOUT, LOG
from mriya.sf_merge import SoapMerge
from mriya.sf_merge_wrapper import SfSoapMergeWrapper, HEADER
from mriya.bulk_data import csv_from_bulk_data, BulkData
from mriya.sql_executor import SqlExecutor, setdatadir
from mriya.job_syntax_extended import JobSyntaxExtended
from mriya.opcsv import CsvWriter
from mriya.sf_merge import SoapException
from mriya_dmt import run_job_from_file
from mockers import mock_oauth, mock_login
import tempfile
import os

# data is sending in specific order, so don't try to reorder data rows ['000001111122222799', '000001111122222789', '000001111122222100']
MERGE_HTTP_RESP = """<?xml version="1.0" encoding="UTF-8"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns="urn:enterprise.soap.sforce.com"><soapenv:Header><LimitInfoHeader><limitInfo><current>4735</current><limit>5000000</limit><type>API REQUESTS</type></limitInfo></LimitInfoHeader></soapenv:Header><soapenv:Body><mergeResponse>\
<result><errors><message>invalid cross reference id</message><statusCode>INVALID_CROSS_REFERENCE_KEY</statusCode></errors><id/><success>false</success></result>\
<result><errors><message>...</message><statusCode>DELETE_FAILED</statusCode></errors><id>000001111122222789</id><success>false</success></result>\
<result><id>000001111122222100</id><mergedRecordIds>000001111122222733</mergedRecordIds><success>true</success></result>\
</mergeResponse></soapenv:Body></soapenv:Envelope>"""

# part of data from MERGE_HTTP_RESP
MERGE_HTTP_PART1_RESP = """<?xml version="1.0" encoding="UTF-8"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns="urn:enterprise.soap.sforce.com"><soapenv:Header><LimitInfoHeader><limitInfo><current>4735</current><limit>5000000</limit><type>API REQUESTS</type></limitInfo></LimitInfoHeader></soapenv:Header><soapenv:Body><mergeResponse>\
<result><errors><message>invalid cross reference id</message><statusCode>INVALID_CROSS_REFERENCE_KEY</statusCode></errors><id/><success>false</success></result>\
<result><errors><message>...</message><statusCode>DELETE_FAILED</statusCode></errors><id>000001111122222789</id><success>false</success></result>\
</mergeResponse></soapenv:Body></soapenv:Envelope>"""

# part of data from MERGE_HTTP_RESP
MERGE_HTTP_PART2_RESP = """<?xml version="1.0" encoding="UTF-8"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns="urn:enterprise.soap.sforce.com"><soapenv:Header><LimitInfoHeader><limitInfo><current>4735</current><limit>5000000</limit><type>API REQUESTS</type></limitInfo></LimitInfoHeader></soapenv:Header><soapenv:Body><mergeResponse>\
<result><id>000001111122222100</id><mergedRecordIds>000001111122222733</mergedRecordIds><success>true</success></result>\
</mergeResponse></soapenv:Body></soapenv:Envelope>"""

# just an example 
MERGE_HTTP_FAULT_RESP = """<?xml version='1.0' encoding='UTF-8'?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <soapenv:Fault>
            <faultcode>soapenv:Client</faultcode>
            <faultstring>......</faultstring>
            <detail/>
        </soapenv:Fault>
    </soapenv:Body>
</soapenv:Envelope>
"""


BULK_DATA_IN = BulkData(fields=('MasterRecordId', 'MergeRecordId'),
                     rows=[('000001111122222789', '000001111122222711'),
                           ('000001111122222799', '000001111122222722'),
                           ('000001111122222100', '000001111122222733'),
                     ])

BULK_DATA_OUT \
    = BulkData(fields=('Id', 'Success', 'StatusCode', 'Message'),
               rows=[(u'000001111122222799', u'false', u'INVALID_CROSS_REFERENCE_KEY', u'invalid cross reference id'),
                     (u'000001111122222789', u'false', 'DELETE_FAILED', '...'),
                     (u'000001111122222100', u'true', u'', u''),
               ])

MERGE_CSV_RES = """Id,Success,StatusCode,Message
"0016100000RsjBbAAJ","true","",""
"0016100000Tl3MFAAZ","false","ENTITY_IS_DELETED","entity is deleted"
"""

@requests_mock.Mocker()
def test_mocked_merge_unsupported_object(m):
    loginit(STDERR)
    fake_bulk_connector = mock.Mock()
    fake_bulk_connector.bulk.sessionid = 'fake-sessionid'
    fake_bulk_connector.bulk.instance_url = 'https://fake-localhost'
    
    m.post(url=requests_mock.ANY, text = '')
    sm = SfSoapMergeWrapper(fake_bulk_connector, 'UnsupportedObject', BULK_DATA_IN, 99)
    assert None == sm.validate()


def fast_merge_mock(m, response_list):
    datadir = tempfile.mkdtemp()
    setdatadir(datadir)
    mock_oauth(m)
    mock_login(m)
    
    # test smaller part of merge flow but checking output
    # Also test how merge splits payload to sequence of chunks
    loginit(STDERR)
    fake_bulk_connector = mock.Mock()
    m.register_uri(method='POST', url='https://fake-localhost/services/Soap/c/37.0',
                   response_list=response_list)
    sm = SfSoapMergeWrapper(fake_bulk_connector, 'Account', BULK_DATA_IN, 2)
    sm.sf_bulk_connector.bulk.sessionid = 'fake-sessionid'
    sm.sf_bulk_connector.instance_url = 'https://fake-localhost'
    res = sm.validate()
    assert res != None
    print res
    bulk_data = sm.run_merge()
    print bulk_data
    assert BULK_DATA_OUT == bulk_data
    
    
@requests_mock.Mocker()    
def test_mocked_merge1(m):
    fast_merge_mock(m, [{'text':MERGE_HTTP_PART1_RESP},
                        {'text':MERGE_HTTP_PART2_RESP}])

@requests_mock.Mocker()
def test_mocked_merge2(m):
    datadir = tempfile.mkdtemp()
    setdatadir(datadir)
    mock_oauth(m)
    mock_login(m)

    # test bigger part of merge flow but don't check output
    m.post(url='https://fake-host.salesforce.com/services/Soap/c/37.0',
           text = MERGE_HTTP_RESP)    
    with open(os.path.join(datadir, 'mergedata.csv'), 'w') as mergedata_f:
        csv_writer = CsvWriter(mergedata_f, False)
        csv_writer.write_csv([BULK_DATA_IN.fields])
        csv_writer.write_csv(BULK_DATA_IN.rows)

    config_filename = 'test-config.ini'
    endpoint_names = {'dst': 'test', 'src': 'test'}
    with open(config_filename) as config_f:
        with open('tests/sql/merge.sql') as job_f:
            run_job_from_file(config_f, job_f, endpoint_names, {}, False, False)

@requests_mock.Mocker()    
def test_internal_merge_erroe(m):
    try:
        fast_merge_mock(m, [{'text':MERGE_HTTP_PART2_RESP},
                            {'text':MERGE_HTTP_PART1_RESP}])
        # it is expected to fail, so assert 0 is like a guard
        assert(0)
    except Exception:
        pass
        
@requests_mock.Mocker()
def test_mocked_merge_error(m):
    datadir = tempfile.mkdtemp()
    setdatadir(datadir)
    mock_oauth(m)
    mock_login(m)

    # test bigger part of merge flow but don't check output
    m.post(url='https://fake-host.salesforce.com/services/Soap/c/37.0',
           text = MERGE_HTTP_FAULT_RESP, status_code=201)
    with open(os.path.join(datadir, 'mergedata.csv'), 'w') as mergedata_f:
        csv_writer = CsvWriter(mergedata_f, False)
        csv_writer.write_csv([BULK_DATA_IN.fields])
        csv_writer.write_csv(BULK_DATA_IN.rows)

    config_filename = 'test-config.ini'
    endpoint_names = {'dst': 'test', 'src': 'test'}
    with open(config_filename) as config_f:
        with open('tests/sql/merge.sql') as job_f:
            try:
                run_job_from_file(config_f, job_f, endpoint_names, {}, False, False)
                # expected to fail
                assert(0)
            except SoapException:
                pass
            
    
if __name__ == '__main__':
    test_resp_parser()
