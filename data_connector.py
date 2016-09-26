from time import sleep
from urlparse import urlparse

__author__ = 'Volodymyr Varchuk'

import beatbox
import logging
import sys
import csv
from project_utils import printProgress, spin
from collections import namedtuple
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk, CsvDictsAdapter
import requests
from json import loads, load, dump


ConnectorParam = namedtuple('ConnectorParam',
                         ['username', 'password', 'url_prefix',
                          'organization_id', 'consumer_key',
                          'consumer_secret', 'token'])

QUERY_LIMIT = 200
session_file = 'sessions.ini'

def get_conn_param(conf_dict):
    param = ConnectorParam(conf_dict['username'].encode('utf-8'),
                           conf_dict['password'].encode('utf-8'),
                           conf_dict['host_prefix'].encode('utf-8'),
                           '',
                           conf_dict['consumer_key'].encode('utf-8'),
                           conf_dict['consumer_secret'].encode('utf-8'),
                           '')
    return param


class SFBeatboxConnector:
    def __init__(self, connector_param, batch_size=1000):
        self.svc = beatbox.PythonClient()
        if (not self.svc):
            print('no connection')
            return
        self.batch_size = batch_size
        logging.info('Process created for batch size {0} '.format(batch_size))
        if connector_param.url_prefix != '':
            self.svc.serverUrl = self.svc.serverUrl.replace('login.', connector_param.url_prefix)
        try:
            self.login_result = self.svc.login(connector_param.username, connector_param.password)
            logging.info('Connected to instance {0}'.format(self.svc.serverUrl))
            print('Connected to instance {0}'.format(self.svc.serverUrl))
        except:
            print(sys.exc_info()[1])


    def fetch_all_data(self, soql):
        print ('\tSOQL for fetching data: ' + soql)
        query_result = self.svc.query(soql)
        rows = query_result['records']
        total_records = query_result['size']
        query_locator = query_result['queryLocator']
        ret = printProgress(len(rows), total_records, prefix='Data loaded', suffix='Complete', decimals=1, barLength=100)
        while query_result['done'] is False and len(rows) < total_records:
            query_result = self.svc.queryMore(query_locator)
            query_locator = query_result['queryLocator']
            rows = rows + query_result['records']
            ret = printProgress(len(rows), total_records, prefix='Data loaded', suffix='Complete', decimals=1, barLength=100)
        if ret == 1:
            printProgress(total_records, total_records, prefix='Data loaded', suffix='Complete', decimals=1, barLength=100)
        return rows


    def write_batch_tocsv(self, csv_name, batch_data, headers, write_header=False):
        if write_header:
            open_mode = "wb"
        else:
            open_mode = "ab"
        with open(csv_name, open_mode) as f_csv:
            writer = csv.DictWriter(f_csv, fieldnames=headers)
            if write_header:
                writer.writeheader()
            for row in batch_data:
                writer.writerow(row)
        return len(batch_data)


    def export_to_csv(self, soql, file_to_export):
        query_result = self.svc.query(soql)
        records_buff = query_result['records']
        total_records = query_result['size']
        query_locator = query_result['queryLocator']
        headers = records_buff[0].keys()
        total_wrote = 0
        write_header = True
        # ret = printProgress(total_wrote, total_records, prefix='Data loaded', suffix='Complete', decimals=1, barLength=100)
        while total_wrote < total_records:
            if len(records_buff) >= self.batch_size or query_result['done'] is True:
                total_wrote = total_wrote + self.write_batch_tocsv(file_to_export,records_buff, headers, write_header=write_header)
                # ret = printProgress(total_wrote, total_records, prefix='Data loaded', suffix='Complete', decimals=1, barLength=100)
                if write_header:
                    write_header = False
                records_buff = []
                logging.info('Batch wrote {0} records'.format(total_wrote))
                if query_result['done'] is True:
                    break
            query_result = self.svc.queryMore(query_locator)
            query_locator = query_result['queryLocator']
            records_buff = records_buff + query_result['records']
        # if ret == 1:
        #     printProgress(total_records, total_records, prefix='Data loaded', suffix='Complete', decimals=1, barLength=100)
        # print('\n')
        return total_wrote

    def update(self, data):
        result = self.svc.update(data)
        logging.info(result)
        return result


    def chunked_create(self, raw_data):
        result = []
        if len(raw_data)> QUERY_LIMIT:
            data_chunk = []
            for row in raw_data:
                if len(data_chunk) == QUERY_LIMIT:
                    result = result + self.svc.create(data_chunk)
                    data_chunk = []
                data_chunk.append(row)
            result = result + self.svc.create(data_chunk)
        else:
            result = self.svc.create(raw_data)
        return result


    def chunked_delete(self, raw_data):
        result = []
        if len(raw_data)> QUERY_LIMIT:
            data_chunk = []
            for row in raw_data:
                if len(data_chunk) == QUERY_LIMIT:
                    result = result + self.svc.delete(data_chunk)
                    data_chunk = []
                data_chunk.append(row)
            result = result + self.svc.delete(data_chunk)
        else:
            result = self.svc.delete(raw_data)
        return result


    def chunked_update(self, raw_data):
        result = []
        if len(raw_data)> QUERY_LIMIT:
            data_chunk = []
            for row in raw_data:
                if len(data_chunk) == QUERY_LIMIT:
                    result = result + self.svc.update(data_chunk)
                    data_chunk = []
                data_chunk.append(row)
            result = result + self.svc.update(data_chunk)
        else:
            result = self.svc.update(raw_data)
        return result


class RESTConnector:
    def __init__(self, connector_param):
        self.connector_param = connector_param
        self.instance_url = 'https://' + connector_param.url_prefix + 'salesforce.com'
        self.token_url = 'https://' + connector_param.url_prefix + 'salesforce.com/services/oauth2/token'
        self.access_token = None
        self.get_token()
        self.bulk = SalesforceBulk(sessionId=self.access_token, host=urlparse(self.instance_url).hostname)


    def check_token(self):
        try:
            job = self.bulk.create_query_job(object, contentType='CSV')
            test_query = 'SELECT ID FROM Account LIMIT 1'
            batch = self.bulk.query(job, test_query)
            self.connector_wait(job,batch,'Query done')
            self.bulk.close_job(job)
            return True
        except:
            return False


    def get_token(self):
        if self.access_token == None:
            cached_token = self.get_cached_token()
            if cached_token:
                self.access_token = cached_token
                if not self.check_token():
                    self.get_oauth2_token()
            else:
                self.get_oauth2_token()
        else:
            self.get_oauth2_token()
        return self.access_token


    def get_oauth2_token(self):
        req_param = {
            'grant_type': 'password',
            'client_id': self.connector_param.consumer_key,
            'client_secret': self.connector_param.consumer_secret,
            'username': self.connector_param.username,
            'password': self.connector_param.password
        }
        result = requests.post(self.token_url, headers={"Content-Type":"application/x-www-form-urlencoded"}, data=req_param)
        result_dict = loads(result.content)
        if 'access_token' in result_dict.keys():
            self.access_token = result_dict['access_token']
            self.save_token()
            return result_dict['access_token']
        else:
            print(result_dict)
            return None


    def get_cached_token(self):
        try:
            tokens_dict = load(open(session_file, 'r'))
        except:
            return None
        if self.connector_param.username in tokens_dict.keys():
            return tokens_dict[self.connector_param.username]
        else:
            return None


    def save_token(self):
        tokens_dict = {}
        try:
            tokens_dict = load(open(session_file, 'r'))
        except:
            pass
        tokens_dict[self.connector_param.username] = self.access_token
        dump(tokens_dict, open(session_file, 'w'))


    def remove_token(self):
        tokens_dict = load(open(session_file, 'r'))
        tokens_dict.pop(self.connector_param.username, None)
        dump(tokens_dict, open(session_file, 'w'))


    def bulk_load(self, object, soql, header_columns=None, csv_file=None):
        try:
            job = self.bulk.create_query_job(object, contentType='CSV')
        except:
            self.access_token = None
            self.get_oauth2_token()
            job = self.bulk.create_query_job(object, contentType='CSV')
        batch = self.bulk.query(job, soql)
        self.connector_wait(job,batch,'Query done')
        self.bulk.close_job(job)

        if csv_file:
            open_mode = 'w'
            with open(csv_file, open_mode) as f_csv:
                writer = csv.DictWriter(f_csv, fieldnames=header_columns)
                writer.writeheader()
                for row in self.bulk.get_batch_result_iter(job, batch, parse_csv=True):
                    writer.writerow(row)
        else:
            data = []
            for row in self.bulk.get_batch_result_iter(job, batch, parse_csv=True):
                data.append(row)
            return data



    def bulk_insert(self, object, data):
        job = self.bulk.create_insert_job(object, contentType='CSV')
        csv_iter = CsvDictsAdapter(iter(data))
        batch = self.bulk.post_bulk_batch(job, csv_iter)
        self.connector_wait(job,batch, 'bulk insert done')
        # do not work should return Id`s of created elements
        # res = self.bulk.get_batch_result_iter(job,batch,parse_csv=False)
        self.bulk.close_job(job)

    def bulk_update(self, object, data):
        job = self.bulk.create_update_job(object, contentType='CSV')
        csv_iter = CsvDictsAdapter(iter(data))
        batch = self.bulk.post_bulk_batch(job, csv_iter)
        self.connector_wait(job, batch, 'bulk update done')
        # do not work shuld return Id`s of created elements
        self.bulk.close_job(job)
        rows = []
        for row in self.get_batch_result_iter(job, batch, parse_csv=False):
            rows.append(row)
        return rows


    def bulk_delete(self, object, where):
        delete_job = self.bulk.create_delete_job(object_name=object)
        delete_batch = self.bulk.bulk_delete(delete_job,object, where)
        self.bulk.wait_for_batch(delete_job,delete_batch)
        print('deletion done')


    def bulk_upsert(self, object, external_id_name, data):
        job = self.bulk.create_upsert_job(object_name=object, external_id_name=external_id_name)
        csv_iter = CsvDictsAdapter(iter(data))
        batch = self.bulk.post_bulk_batch(job, csv_iter)
        self.connector_wait(job, batch, 'upserting done')
        self.bulk.close_job(job)
        rows = []
        for row in self.get_batch_result_iter(job, batch, parse_csv=False):
            rows.append(row)
        return rows


    def connector_wait(self, job, batch, ending_message=''):
        wait_message = 'Wait for job done'
        clock = 0
        while True:
            if clock == 10:
                clock = 0
                if self.bulk.is_batch_done(job, batch):
                    break
            sleep(0.5)
            clock = clock + 1
            spin(wait_message)
        print('\r' + ending_message.ljust( len(ending_message) if len(ending_message) > len(wait_message) + 4 else len(wait_message) + 4))
        self.bulk.wait_for_batch(job, batch)


    def get_batch_result_iter(self, job_id, batch_id, parse_csv=False,
                              logger=None):
        """

        **** This code snippet was taken from salesforce bulk library ****

        Return a line interator over the contents of a batch result document. If
        csv=True then parses the first line as the csv header and the iterator
        returns dicts.
        """
        status = self.bulk.batch_status(job_id, batch_id)
        if status['state'] != 'Completed':
            return None
        elif logger:
            if 'numberRecordsProcessed' in status:
                logger("Bulk batch %d processed %s records" %
                       (batch_id, status['numberRecordsProcessed']))
            if 'numberRecordsFailed' in status:
                failed = int(status['numberRecordsFailed'])
                if failed > 0:
                    logger("Bulk batch %d had %d failed records" %
                           (batch_id, failed))
        print(self.bulk.headers())
        uri = self.bulk.endpoint + \
            "/job/%s/batch/%s/result" % (job_id, batch_id)
        r = requests.get(uri, headers=self.bulk.headers(), stream=True)

        # print(type(r))
        # print(r.text)
        # print(r.keys())
        # result_id = r.text.split("<result>")[1].split("</result>")[0]

        # uri = self.bulk.endpoint + \
        #     "/job/%s/batch/%s/result/%s" % (job_id, batch_id, result_id)
        # r = requests.get(uri, headers=self.bulk.headers(), stream=True)

        if parse_csv:
            return csv.DictReader(r.iter_lines(chunk_size=2048), delimiter=",",
                                  quotechar='"')
        else:
            return r.iter_lines(chunk_size=2048)