
__author__ = 'Volodymyr Varchuk, Yaroslav Litvinov'

from collections import namedtuple
import os
import requests
from json import loads, load, dump
from mriya.sf_bulk_connector import SfBulkConnector
from mriya.config import *

ConnectorParam = namedtuple('ConnectorParam',
                         ['username', 'password', 'url_prefix',
                          'production', 'consumer_key',
                          'consumer_secret', 'token'])

def get_conn_param(conf_dict):
    param = ConnectorParam(conf_dict[USERNAME_SETTING].encode('utf-8'),
                           conf_dict[PASSWORD_SETTING].encode('utf-8'),
                           conf_dict[HOST_PREFIX_SETTING].encode('utf-8'),
                           conf_dict.getboolean(PRODUCTION_SETTING),
                           conf_dict[CONSUMER_KEY_SETTING].encode('utf-8'),
                           conf_dict[CONSUMER_SECRET_SETTING].encode('utf-8'),
                           '')
    return param

def conn_param_set_token(conn_param, access_token):
    assert len(conn_param) == 7
    new_conn_param_list = list(conn_param)
    new_conn_param_list[6] = access_token
    return ConnectorParam._make(new_conn_param_list)

def create_bulk_connector(config, setting_name):
    sessions_file_name = config[DEFAULT_SETTINGS_SECTION][SESSIONS_SETTING]
    conn_param = get_conn_param(config[setting_name])
    auth_token = AuthToken(conn_param, sessions_file_name)
    conn_param = auth_token.conn_param_with_token()
    conn = SfBulkConnector(conn_param)
    return conn

class AuthToken(object):
    def __init__(self, conn_param, sessions_file):
        self.conn_param = conn_param
        self.sessions_file = sessions_file
        self.conn_param = conn_param_set_token(conn_param, self.get_token())

    def conn_param_with_token(self):
        return self.conn_param

    @staticmethod
    def oauth2_token(conn_param):
        req_param = {
            'grant_type': 'password',
            'client_id': conn_param.consumer_key,
            'client_secret': conn_param.consumer_secret,
            'username': conn_param.username,
            'password': conn_param.password
        }
    
        token_url_fmt = 'https://{url_prefix}salesforce.com/services/oauth2/token'
        token_url = token_url_fmt.format(url_prefix=conn_param.url_prefix)

        result = requests.post(
            token_url,
            headers={"Content-Type":"application/x-www-form-urlencoded"},
            data=req_param)

        result_dict = loads(result.content)
        if 'access_token' in result_dict.keys():
            return result_dict['access_token']
        else:
            Exception("Can't obtain oauth token", result_dict)

    def get_token(self):
        token = self.get_cached_token()
        if not token:
            token = AuthToken.oauth2_token(self.conn_param)
            self.save_token(token)
        return token

    def get_cached_token(self):
        try:
            tokens_dict = load(open(self.sessions_file, 'r'))
        except:
            return None
        if self.conn_param.username in tokens_dict.keys():
            return tokens_dict[self.conn_param.username]
        else:
            return None

    def save_token(self, token):
        tokens_dict = {}
        try:
            tokens_dict = load(open(self.sessions_file, 'r'))
        except:
            pass
        tokens_dict[self.conn_param.username] = token
        dump(tokens_dict, open(self.sessions_file, 'w'))
        


