__author__ = 'Volodymyr Varchuk'

import beatbox
import logging
import sys
import csv
from project_utils import printProgress
from collections import namedtuple



ConnectorParam = namedtuple('ConnectorParam',
                         ['user', 'password', 'url_prefix',
                          'organization_id', 'consumer_key', 'consumer_secret', 'token'])

QUERY_LIMIT = 200


class SFBeatboxConnector:
    def __init__(self, connector_param, batch_size=1000):
        self.svc = beatbox.PythonClient()
        self.batch_size = batch_size
        logging.info('Process created for batch size {0} '.format(batch_size))
        if connector_param.url_prefix != '':
            self.svc.serverUrl = self.svc.serverUrl.replace('login.', connector_param.url_prefix)
        try:
            self.login_result = self.svc.login(connector_param.user, connector_param.password)
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
        self.instance_url = 'https://' + connector_param.url_prefix + 'salesforce.com'
        self.token_url = 'https://' + connector_param.url_prefix + 'salesforce.com/services/oauth2/token'
        self.access_token = ''



    def check_token(self):
