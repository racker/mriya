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

__author__ = "Yaroslav Litvinov"

import mock
import requests
import requests_mock
from collections import namedtuple

OATH_RESP = '{"access_token":"someaccesstoken.blablabla30xzbENMJgDaL.eyU0wIUi61Uc41XBgv06CXTrpR5a7iN9abDDIaYVyDundZP35734768NoKW1RMWf.tokeniCs","instance_url":"https://fake-host.salesforce.com","id":"https://test.salesforce.com/id/00Dn00000000YB8EAM/005n00000026666666","token_type":"Bearer","issued_at":"1498150538789","signature":"vc+uz6456234627788845836845848++Rasd43Xop6Q="}'

LOGIN_RESP = '<?xml version="1.0" encoding="UTF-8"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns="urn:partner.soap.sforce.com" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><soapenv:Body><loginResponse><result><metadataServerUrl>https://fake-host.salesforce.com/services/Soap/m/37.0/00Dn00000000YB8</metadataServerUrl><passwordExpired>true</passwordExpired><sandbox>true</sandbox><serverUrl>https://fake-host.salesforce.com/services/Soap/u/37.0/00Dn00000000YB8</serverUrl><sessionId>fakefake.somesessionid</sessionId><userId>005n0003563456dAAE</userId><userInfo><accessibilityMode>false</accessibilityMode><currencySymbol xsi:nil="true"/><orgAttachmentFileSizeLimit>7340032</orgAttachmentFileSizeLimit><orgDefaultCurrencyIsoCode xsi:nil="true"/><orgDisallowHtmlAttachments>false</orgDisallowHtmlAttachments><orgHasPersonAccounts>false</orgHasPersonAccounts><organizationId>00Dn0346745746778M</organizationId><organizationMultiCurrency>true</organizationMultiCurrency><organizationName>Rackspace</organizationName><profileId>00e50000007676767Q</profileId><roleId xsi:nil="true"/><sessionSecondsValid>28800</sessionSecondsValid><userDefaultCurrencyIsoCode>USD</userDefaultCurrencyIsoCode><userEmail>dr.who@whois.com</userEmail><userFullName>Dr. Who</userFullName><userId>005n00000026666666</userId><userLanguage>en_US</userLanguage><userLocale>en_US</userLocale><userName>dr.who@whois.com</userName><userTimeZone>America/Chicago</userTimeZone><userType>Standard</userType><userUiSkin>Theme3</userUiSkin></userInfo></result></loginResponse></soapenv:Body></soapenv:Envelope>'

#operation=insert/update/query
#state=Open/Closed
#jobid=
JOB_INFO_RESP_FMT = """<?xml version="1.0" encoding="UTF-8"?><jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
 <id>{jobid}</id>
 <operation>{operation}</operation>
 <object>Account</object>
 <createdById>005n00000026666666</createdById>
 <createdDate>2017-06-22T19:06:01.000Z</createdDate>
 <systemModstamp>2017-06-22T19:06:01.000Z</systemModstamp>
 <state>{state}</state>
 <concurrencyMode>Parallel</concurrencyMode>
 <contentType>CSV</contentType>
 <numberBatchesQueued>0</numberBatchesQueued>
 <numberBatchesInProgress>0</numberBatchesInProgress>
 <numberBatchesCompleted>0</numberBatchesCompleted>
 <numberBatchesFailed>0</numberBatchesFailed>
 <numberBatchesTotal>0</numberBatchesTotal>
 <numberRecordsProcessed>0</numberRecordsProcessed>
 <numberRetries>0</numberRetries>
 <apiVersion>38.0</apiVersion>
 <numberRecordsFailed>0</numberRecordsFailed>
 <totalProcessingTime>0</totalProcessingTime>
 <apiActiveProcessingTime>0</apiActiveProcessingTime>
 <apexProcessingTime>0</apexProcessingTime>
</jobInfo>"""

#state=Queued/InProgress/Completed
#jobid=
#batchid=
BATCH_INFO_RESP_FMT="""<?xml version="1.0" encoding="UTF-8"?><batchInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
 <id>{batchid}</id>
 <jobId>{jobid}</jobId>
 <state>{state}</state>
 <createdDate>2017-06-22T19:06:01.000Z</createdDate>
 <systemModstamp>2017-06-22T19:06:01.000Z</systemModstamp>
 <numberRecordsProcessed>0</numberRecordsProcessed>
 <numberRecordsFailed>0</numberRecordsFailed>
 <totalProcessingTime>0</totalProcessingTime>
 <apiActiveProcessingTime>0</apiActiveProcessingTime>
 <apexProcessingTime>0</apexProcessingTime>
</batchInfo>"""

def mock_oauth(m):
    m.post(url="https://fake-host.salesforce.com/services/oauth2/token", text = OATH_RESP)
    
def mock_login(m):
    m.post(url="https://test.salesforce.com/services/Soap/u/37.0", text = LOGIN_RESP)

MockData = namedtuple('MockData', ['req', 'resp'])
    
class SFMock(object):
    """ Http Mock engine itself, it just creates a sequences of responses as a side_effect() """
    baseurl='https://fake-host.salesforce.com/services/async/38.0'
    def __init__(self, host):
        self.host = host
        self.mocks = []
        
    def addmock_insert_update_delete(self, operation, jobid, batchid, resp):
        self._open_job(operation, jobid)
        self._batch_info(state='Queued', batchid=batchid, new=True)
        self._batch_info(state='InProgress', batchid=batchid)
        self._batch_info(state='Completed', batchid=batchid)
        self._batch_simple_result(resp)
        self._close_job()

    def addmock_query(self, operation, jobid, batchid, result):
        self._open_job(operation, jobid)
        self._batch_info(state='Queued', batchid=batchid, new=True)
        self._batch_info(state='Completed', batchid=batchid)
        self._batch_chunked_result(result)
        self._close_job()

    def addmock_bad_query(self, operation, jobid, batchid, result):
        self._open_job(operation, jobid)
        self._batch_info(state='Queued', batchid=batchid, new=True)
        self._batch_info(state='Failed', batchid=batchid)
        self._batch_info(state='Closed', batchid=batchid)
        self._close_job()

    def side_effect(self):
        return [x.resp for x in self.mocks]        
        
    def _batch_simple_result(self, resp):
        self.mocks.append(MockData(
            req='%s/job/%s/batch/%s/result' % (self.baseurl, self.jobid, self.batchid),
            resp=resp))
        
    def _batch_chunked_result(self, results_data):
        results = ''
        for id_data in results_data:
            results += '<result>%s</result>' % id_data[0]
        self.mocks.append(MockData(
            req='%s/job/%s/batch/%s/result' % (self.baseurl, self.jobid, self.batchid),
            resp='<result-list xmlns="http://www.force.com/2009/06/asyncapi/dataload">%s</result-list>'
            % results ))
        for id_data in results_data:
            self.mocks.append(MockData(
                req='%s/job/%s/batch/%s/result/%s' % (self.baseurl,
                                                      self.jobid, self.batchid, id_data[0]),
                resp=id_data[1]))
        
    def _open_job(self, operation, jobid):
        self.operation = operation
        self.jobid = jobid
        self.mocks.append(MockData(req='%s/job' % (self.baseurl),
                                   resp=JOB_INFO_RESP_FMT.format(state='Open',
                                                                 jobid=self.jobid,
                                                                 operation=self.operation)))
    def _close_job(self):
        self.mocks.append(MockData(req='%s/job/%s' % (self.baseurl, self.jobid),
                                   resp=JOB_INFO_RESP_FMT.format(state='Closed',
                                                                 jobid=self.jobid,
                                                                 operation=self.operation)))
    def _batch_info(self, state, batchid, new=False):
        self.batchid = batchid
        if new:
            self.mocks.append(
                MockData(req='%s/job/%s/batch' % (self.baseurl, self.jobid),
                         resp=BATCH_INFO_RESP_FMT.format(state=state,
                                                         jobid=self.jobid,
                                                         batchid=self.batchid)))
        else:
            self.mocks.append(
                MockData(req='%s/job/%s/batch/%s' % (self.baseurl, self.jobid, self.batchid),
                         resp=BATCH_INFO_RESP_FMT.format(state=state,
                                                         jobid=self.jobid,
                                                         batchid=self.batchid)))

        
def mock_insert_load_delete(mock_docall, m):
    mock_oauth(m)
    mock_login(m)
    http_mock = SFMock('fake-host')
    
    insert_resp=""""Id","Success","Created","Error"
"001n000000HDYkvAAH","true","true",""
"001n000000HDYkwAAH","true","true",""
"001n000000HDYkxAAH","true","true",""
"001n000000HDYkyAAH","true","true",""
"""
    query_resp_part1 = '''"Name","Account_Birthday__c","Billing_Address__c","Type"
"mriya","","Street_Billing_Address_CO9S63EMH4","Account"
"mriya","2015-05-15","Street_Billing_Address ""PICGZSLC0F""","Account"'''

    query_resp_part2 = '''"Name","Account_Birthday__c","Billing_Address__c","Type"
"mriya","2005-08-13","Street_Billing_Address
2YNSCQEHFX","Account"
"mriya","2000-10-23","Street_Billing_Address, 7VLWJ7CMQX","Account"'''

    delete_resp = '''Id,Success,Created,Error
"001n000000HDYkvAAH","true","false",""'''

    http_mock.addmock_insert_update_delete(
        operation='insert', jobid='750n00000020o33AAA', batchid='751n00000029qH0AAI',
        resp=insert_resp)
    
    http_mock.addmock_query(
        operation='query', jobid='750n00000020o33BBB', batchid='751n00000029q000AI',
        result=[('752n0000000yXIm', query_resp_part1), ('752n0000000yYIm', query_resp_part2)])

    http_mock.addmock_insert_update_delete(
        operation='delete', jobid='750n00000020o33CCC', batchid='751n00000029qX0AAI',
        resp=delete_resp)
   
    # mock install
    mock_docall.side_effect = http_mock.side_effect()

def mock_empty_query_res(mock_docall, m):
    mock_oauth(m)
    mock_login(m)
    http_mock = SFMock('fake-host')
    empty_resp = 'Records not found for this query'

    http_mock.addmock_query(
        operation='query', jobid='750n00000020o33DDD', batchid='751n00000029q000AX',
        result=[('752n0000000yZIm', empty_resp)])
    
    # mock install
    mock_docall.side_effect = http_mock.side_effect()

def mock_bad_response(mock_job_is_completed, mock_docall, m):
    mock_oauth(m)
    mock_login(m)
    http_mock = SFMock('fake-host')
    empty_resp = 'Records not found for this query'

    # bad query response
    http_mock.addmock_bad_query(
        operation='query', jobid='750n00000020o33DDD', batchid='751n00000029q000AX',
        result=[('752n0000000yZIm', empty_resp)])

    # mock install
    mock_docall.side_effect = http_mock.side_effect()
    Bulk.job_is_completed()
    mock_job_is_completed.assert_any_call()
    
def mock_insert_update(mock_docall, m):
    mock_oauth(m)
    mock_login(m)
    http_mock = SFMock('fake-host')
    
    insert_resp='''"Id","Success","Created","Error"
"001n000000HLVWhAAP","true","true",""
'''
    query_resp='''"Id","Name","Account_Birthday__c","Billing_Address__c","Type"
"001n000000HLVWhAAP","mriya","","Street_Billing_Address_CO9S63EMH4","Account"
'''
    update_resp='''"Id","Success","Created","Error"
"001n000000HLVWhAAP","true","false",""
'''
    query2_resp='''"Id","Name","Account_Birthday__c","Billing_Address__c","Type"
"001n000000HLVWhAAP","BLA-BLA-BLA","","Street_Billing_Address_CO9S63EMH4","Account"
'''
    http_mock.addmock_insert_update_delete(
        operation='insert', jobid='750n00000021GUWAA2', batchid='751n0000002ABxRAAW',
        resp=insert_resp)

    http_mock.addmock_query(
        operation='query', jobid='750n00000021GUqAAM', batchid='751n0000002ABxlAAG',
        result=[('752n0000000ylit', query_resp)])
    
    http_mock.addmock_insert_update_delete(
        operation='update', jobid='750n00000021GUvAAM', batchid='751n0000002ABxqAAG',
        resp=update_resp)

    http_mock.addmock_query(
        operation='query', jobid='750n00000021GV5AAM', batchid='751n0000002ABy5AAG',
        result=[('752n0000000ylj8', query2_resp)])

    # mock install
    mock_docall.side_effect = http_mock.side_effect()
    
def mock_var_csv(mock_docall, m):
    mock_oauth(m)
    mock_login(m)
    http_mock = SFMock('fake-host')

    query_resp='''"Id"
"001n0000009bCMEAA2"
'''
    http_mock.addmock_query(
        operation='query', jobid='750n00000021GfPAAU', batchid='751n0000002AC2MAAW',
        result=[('752n0000000yllO', query_resp)])
    
    # mock install
    mock_docall.side_effect = http_mock.side_effect()

def mock_job_controller(mock_docall, m):
    mock_oauth(m)
    mock_login(m)
    http_mock = SFMock('fake-host')

    query_resp = '''Id,Account_Birthday__c,Name,Alexa__c
001n000000HLWvbAAH,12/12/2017,somename,somealexa
'''
    insert_resp = '''"Id","Success","Created","Error"
"001n000000HLWvbAAH","true","true",""
'''
    update_resp = '''"Id","Success","Created","Error"
"001n0000009uyaDAAQ","true","false",""
'''
    update2_resp = '''"Id","Success","Created","Error"
"001n0000009uyaDAAQ","true","false",""
'''
    query2_resp = '''"Alexa__c"
"hello

2"
'''
    http_mock.addmock_query(
        operation='query', jobid='750n00000021GB3AAE', batchid='751n0000002AB5GAAW',
        result=[('752n0000000ylnP', query_resp)])
    
    http_mock.addmock_insert_update_delete(
        operation='insert', jobid='750n00000021GsnAAE', batchid='751n0000002AC51AAG',
        resp=insert_resp)

    http_mock.addmock_insert_update_delete(
        operation='update', jobid='750n00000021GutAAE', batchid='751n0000002AC56AAG',
        resp=update_resp)

    http_mock.addmock_insert_update_delete(
        operation='update', jobid='750n00000021GuyAAE', batchid='751n0000002AC5BAAW',
        resp=update2_resp)

    http_mock.addmock_query(
        operation='query', jobid='750n00000021Gv3AAE', batchid='751n0000002AC5GAAW',
        result=[('752n0000000ylnP', query2_resp)])

    http_mock.addmock_insert_update_delete(
        operation='update', jobid='750n00000021GufAAE', batchid='751n0000002AC5BABW',
        resp=update2_resp)
    
    # mock install
    mock_docall.side_effect = http_mock.side_effect()
    
