#!/usr/bin/env python

__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

import logging
import sys
import pprint
from logging import getLogger
from configparser import ConfigParser
from mriya.data_connector import RESTConnector
from mriya.data_connector import get_conn_param
from mriya.data_connector import conn_param_set_token
from mriya.data_connector import AuthToken
from mriya.test_data_generator import DataGenerator
from mriya.test_data_generator import data_struct
from mriya.test_data_generator import defaults
from mriya.test_data_generator import prefixes
from mriya.sf_bulk_connector import SfBulkConnector
from mriya.bulk_data import csv_from_list_of_dicts
from mriya.bulk_data import parse_batch_res_data
from mriya.bulk_data import BulkData

config_file = 'test-config.ini'

def setup():
    logging.basicConfig(level=logging.DEBUG,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)-8s %(message)s')
    getLogger(__name__).setLevel(logging.DEBUG)
    getLogger(__name__).info("Log created")
    # These two lines enable debugging at httplib level
    # (requests->urllib3->http.client) You will see the REQUEST,
    # including HEADERS and DATA, and RESPONSE with HEADERS but
    # without DATA.  The only thing missing will be the response.body
    # which is not logged.
    try:
        import http.client as http_client
    except ImportError:
        # Python 2
        import httplib as http_client
    http_client.HTTPConnection.debuglevel = 1


def get_bulk_conn(section_name):
    config = ConfigParser()
    with open(config_file, 'r') as conf_file:
        config.read_file(conf_file)
    conn_param = get_conn_param(config[section_name])
    print "ok"
    auth_token = AuthToken(conn_param, 'sessions.ini')
    conn_param = auth_token.conn_param_with_token()
    conn = SfBulkConnector(conn_param)
    return conn

def test_insert():
    conn = get_bulk_conn()

    # WHERE Id = '001n0000009bI3MAAU'
    csv_rows = conn.bulk_load('Contact', "SELECT id FROM Contact")
    with open('Contacts.csv', 'w') as contacts_f:
        for row in csv_rows:
            contacts_f.write(row)
            contacts_f.write('\n')

def split_list(maxs, src):
    return [src[i:i + maxs] for i in xrange(0, len(src), maxs)]

def main():
    # run soql query to src tables -> get IDS
    conn_uat = get_bulk_conn('uat')
    conn_new_uat = get_bulk_conn('new uat')

    # select old ids to migrate
    csv_rows = conn_uat.bulk_load('Rackspace_Contacts__c',
                                  "SELECT id FROM Rackspace_Contacts__c \
WHERE New_Record_id__c = null and (id='a013800000thxF5AAI' OR id='a013800000thxF9AAI' OR id='a013800000thxFAAAY' OR id='a013800000thxIhAAI' OR id='a013800000thxImAAI' OR id='a013800000thxKYAAY' OR id='a013800000thxKdAAI' OR id='a013800000thxKiAAI' OR id='a013800000thxKnAAI' OR id='a013800000thxKsAAI')")
    old_contacts = parse_batch_res_data(csv_rows)
    old_contact_ids = [i[0] for i in old_contacts.rows]
    old_contact_ids.sort()
    print len(old_contact_ids), old_contact_ids

    # select old ids from new
    new_csv_rows = conn_new_uat.bulk_load('Account_Contact_Role__c',
                           'SELECT id, Old_Contact_id__c FROM \
Account_Contact_Role__c WHERE Old_Contact_id__c != null')
    old_contacts_from_new = parse_batch_res_data(new_csv_rows)
    old_contact_ids_from_new = [i[0] for i in old_contacts_from_new.rows]

    # get old ids not present in new
    old_ids_to_insert = [i for i in old_contact_ids \
                         if i not in old_contact_ids_from_new ]
    print len(old_ids_to_insert), old_ids_to_insert

    if old_ids_to_insert:
        # split ids into sublists for bulk inserts
        split_size = 4
        splitted_old_contact_ids = split_list(split_size, old_ids_to_insert)
        for one_old_contact_ids in splitted_old_contact_ids:
            print one_old_contact_ids
            rows = []
            for old_id in one_old_contact_ids:
                rows.append((old_id))
            bulk_ids = BulkData(['Old_Contact_Id__c'], rows)
            csv_to_insert = csv_from_bulk_data(bulk_ids)
            conn_new_uat.bulk_insert('Contact', csv_to_insert)
    
    # select id, old ids from new
    new_ids_csv_rows = conn_new_uat.bulk_load('Account_Contact_Role__c',
                           'SELECT id, Old_Contact_id__c FROM \
Account_Contact_Role__c WHERE Old_Contact_id__c != null')
    data_from_new = parse_batch_res_data(new_ids_csv_rows)
    print data_from_new

if __name__ == '__main__':
    setup()
    main()
    
