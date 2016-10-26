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
import urllib
import urllib2

from pprint import pformat
from sfbulk.exceptions import BulkException


class Callout(object):
    """
    Helper class to easily do GET / POST method along with the encoded data.
    """
    headerAuth = None
    sessionid = None

    AUTHORIZATION = u'Authorization'

    def __init__(self, header={}, logger=None):
        if self.AUTHORIZATION in header:
            self.headerAuth = header['Authorization']
        self.logger = logger

    def docall(self, url, method, tdata=None, headers=None):
        """
        Initiate http call (get / post) along with the data.

        @type: string
        @param url: server url information for the callout destination
        @type: string
        @param method: "GET" or "POST"
        @type: string
        @param tdata: data information which will be submitted on the BODY
        @type: headers
        @param headers: header information
        """
        handler = urllib2.HTTPHandler()
        opener = urllib2.build_opener(handler)
        if tdata is not None:
            if type(tdata) == dict:
                data = urllib.urlencode(tdata)
            else:
                data = tdata
        else:
            data = urllib.urlencode({})
        request = urllib2.Request(url, data)

        if headers is not None:
            for keyh, valueh in headers.iteritems():
                request.add_header(keyh, valueh)
        request.get_method = lambda: method

        try:
            connection = opener.open(request)
        except urllib2.HTTPError as e:
            self.logger.info(
                "Request url: \n %s" % pformat(request.get_full_url()))
            self.logger.info(
                "Request headers: \n %s" % pformat(request.headers))
            self.logger.info("Request data: \n %s" % pformat(request.data))
            message = "Http error occurred on doing API call: %s" % e
            raise BulkException(message)
        return connection.read()
