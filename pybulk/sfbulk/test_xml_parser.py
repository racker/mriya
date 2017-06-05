#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from sfbulk.utils_xml import parseXMLResult

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

    
if __name__ == '__main__':
    test_one_result()
    test_many_results()
    test_big()
