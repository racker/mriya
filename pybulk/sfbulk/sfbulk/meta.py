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
from sfbulk import sf


CREATE_CUSTOM_FIELD_SOAP_REQUEST_HEADERS = {
    u'content-type': 'text/xml',
    u'charset': 'UTF-8',
    u'SOAPAction': 'create'
}

UPDATE_CUSTOM_FIELD_SOAP_REQUEST_HEADERS = {
    u'content-type': 'text/xml',
    u'charset': 'UTF-8',
    u'SOAPAction': 'update'
}

CREATE_CUSTOM_FIELD_SOAP_REQUEST_BODY = \
    """<?xml version="1.0" encoding="UTF-8"?>
    <soapenv:Envelope
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:apex="http://soap.sforce.com/2006/08/apex"
            xmlns:cmd="http://soap.sforce.com/2006/04/metadata"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <soapenv:Header>
            <cmd:SessionHeader>
                <cmd:sessionId>{sessionid}</cmd:sessionId>
            </cmd:SessionHeader>
        </soapenv:Header>
        <soapenv:Body>
            <create xmlns="http://soap.sforce.com/2006/04/metadata">
                <metadata xsi:type="CustomField">
                    <fullName>{object_type}.{name}__c</fullName>
                    <label>{name}</label>
                    <length>{length}</length>
                    <type>{type_}</type>
                    <required>{required}</required>
                </metadata>
            </create>
        </soapenv:Body>
    </soapenv:Envelope>"""

CREATE_CUSTOM_FIELD_LONG_SOAP_REQUEST_BODY = \
    """<?xml version="1.0" encoding="UTF-8"?>
    <soapenv:Envelope
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:apex="http://soap.sforce.com/2006/08/apex"
            xmlns:cmd="http://soap.sforce.com/2006/04/metadata"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <soapenv:Header>
            <cmd:SessionHeader>
                <cmd:sessionId>{sessionid}</cmd:sessionId>
            </cmd:SessionHeader>
        </soapenv:Header>
        <soapenv:Body>
            <create xmlns="http://soap.sforce.com/2006/04/metadata">
                <metadata xsi:type="CustomField">
                    <fullName>{object_type}.{name}__c</fullName>
                    <label>{name}</label>
                    <length>{length}</length>
                    <type>{type_}</type>
                    <visibleLines>3</visibleLines>
                    <required>{required}</required>
                </metadata>
            </create>
        </soapenv:Body>
    </soapenv:Envelope>"""

UPDATE_CUSTOM_FIELD_SOAP_REQUEST_BODY = \
    """<?xml version="1.0" encoding="UTF-8"?>
    <soapenv:Envelope
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:apex="http://soap.sforce.com/2006/08/apex"
            xmlns:cmd="http://soap.sforce.com/2006/04/metadata"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <soapenv:Header>
            <cmd:SessionHeader>
                <cmd:sessionId>{sessionid}</cmd:sessionId>
            </cmd:SessionHeader>
        </soapenv:Header>
        <soapenv:Body>
            <update xmlns="http://soap.sforce.com/2006/04/metadata">
                <UpdateMetadata xsi:type="CustomField">
                    <currentName>{object_type}.{name}__c</currentName>
                    <metadata xsi:type="CustomField">
                    <fullName>{object_type}.{name}__c</fullName>
                        <label>{name}</label>
                        <length>{length}</length>
                        <type>{type_}</type>
                        <required>{required}</required>
                    </metadata>
                </UpdateMetadata>
            </update>
        </soapenv:Body>
    </soapenv:Envelope>"""

UPDATE_CUSTOM_FIELD_LONG_SOAP_REQUEST_BODY = \
    """<?xml version="1.0" encoding="UTF-8"?>
    <soapenv:Envelope
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:apex="http://soap.sforce.com/2006/08/apex"
            xmlns:cmd="http://soap.sforce.com/2006/04/metadata"
            xmlns:xsd="http://www.w3.org/2001/XMLSchema"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <soapenv:Header>
            <cmd:SessionHeader>
                <cmd:sessionId>{sessionid}</cmd:sessionId>
            </cmd:SessionHeader>
        </soapenv:Header>
        <soapenv:Body>
            <update xmlns="http://soap.sforce.com/2006/04/metadata">
                <UpdateMetadata xsi:type="CustomField">
                    <currentName>{object_type}.{name}__c</currentName>
                    <metadata xsi:type="CustomField">
                    <fullName>{object_type}.{name}__c</fullName>
                        <label>{name}</label>
                        <length>{length}</length>
                        <type>{type_}</type>
                        <visibleLines>3</visibleLines>
                        <required>{required}</required>
                    </metadata>
                </UpdateMetadata>
            </update>
        </soapenv:Body>
    </soapenv:Envelope>"""


class sfMeta(sf):

    def __init__(self):
        super(sfMeta, self).__init__()

    def login(self, *args, **kwargs):
        super(sfMeta, self).login(*args, **kwargs)
        self.meta_server = self.soap_server.replace('/u/', '/m/')

    def create_field(self,
                     object_type, name, type_, length, required=u'false'):
        body = CREATE_CUSTOM_FIELD_SOAP_REQUEST_BODY
        if type_ == 'LongTextArea':
            body = CREATE_CUSTOM_FIELD_LONG_SOAP_REQUEST_BODY

        create_custom_field_soap_request_body = \
            body\
            .format(sessionid=self.sessionid,
                    object_type=object_type,
                    name=name,
                    type_=type_,
                    length=length,
                    required=required)

        return self._send_create_custom_field_request(
            create_custom_field_soap_request_body)

    def update_field(self,
                     object_type, name, type_, length, required=u'false'):

        body = UPDATE_CUSTOM_FIELD_SOAP_REQUEST_BODY
        if type_ == 'LongTextArea':
            body = UPDATE_CUSTOM_FIELD_LONG_SOAP_REQUEST_BODY

        update_custom_field_soap_request_body = \
            body\
            .format(sessionid=self.sessionid,
                    object_type=object_type,
                    name=name,
                    type_=type_,
                    length=length,
                    required=required)

        return self._send_create_custom_field_request(
            update_custom_field_soap_request_body)

    def _send_create_custom_field_request(
            self, create_custom_field_soap_request_body):
        return requests.post(self.meta_server,
                             create_custom_field_soap_request_body,
                             headers=CREATE_CUSTOM_FIELD_SOAP_REQUEST_HEADERS)

    def _send_update_custom_field_request(
            self, update_custom_field_soap_request_body):
        return requests.post(self.meta_server,
                             update_custom_field_soap_request_body,
                             headers=UPDATE_CUSTOM_FIELD_SOAP_REQUEST_HEADERS)
