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


import requests

class BaseBulkConnector(object):
    
    def __init__(self, conn_param):
        self.conn_param = conn_param
        self.instance_url = 'https://{prefix}salesforce.com'.format(
            prefix=conn_param.url_prefix)

    def get_oauth2_token(self):
        req_param = {
            'grant_type': 'password',
            'client_id': self.conn_param.consumer_key,
            'client_secret': self.conn_param.consumer_secret,
            'username': self.conn_param.username,
            'password': self.conn_param.password
        }
        result = requests.post(
            self.token_url,
            headers={"Content-Type":"application/x-www-form-urlencoded"},
            data=req_param)
        result_dict = loads(result.content)
        if 'access_token' in result_dict.keys():
            return result_dict['access_token']
        else:
            print(result_dict)
            return None

    def fetch_token(self):
        token = self.get_oauth2_token()
        

    def bulk_insert(self, objname, list_of_dicts_data):
        """ return -- objects' ids"""
        raise NotImplementedError('You need to define a sync method!')
