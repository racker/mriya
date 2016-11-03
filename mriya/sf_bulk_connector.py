
__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from sfbulk import Bulk
from logging import getLogger, DEBUG
from time import sleep
from mriya.base_connector import BaseBulkConnector
from mriya.bulk_data import parse_batch_res_data
from mriya.log import loginit

class SfBulkConnector(BaseBulkConnector):

    def __init__(self, conn_param):
        super(SfBulkConnector, self).__init__(conn_param)
        loginit(__name__)
        # initialize bulk
        self.bulk = Bulk(self.instance_url)
        self.bulk.login(username=self.conn_param.username,
                        password=self.conn_param.password,
                        security_token=self.conn_param.token,
                        #sf_version=38.0,
                        sandbox=True)

    def handle_batch_error(self, batch_id):
        """ Handle bulk error, when no records processed """
        batch_res = None
        if batch_id in self.bulk.jobinfo.batch:
            info = self.bulk.jobinfo.batch[batch_id]
            if info['state'] != 'Completed':
                getLogger(__name__).error('%s bachFailed %s',
                                          batch_id,
                                          info['stateMessage'])

    def handle_op_returning_ids(self, opname, res):
        result_ids = parse_batch_res_data(res)
        id_idx = result_ids.fields.index('Id')
        success_idx = result_ids.fields.index('Success')
        error_idx = result_ids.fields.index('Error')
        for item in result_ids.rows:
            if item[success_idx] != 'true':
                getLogger(__name__).error('Batch %s: Id=%s, error:%s',
                                          opname, item[id_idx],
                                          item[error_idx])
        return result_ids

    @staticmethod
    def batch_ranges(lines_count, batch_size):
        batches = []
        batch_end = -1
        left = lines_count
        while left:
            size = min(left,batch_size)
            batch_begin = batch_end+1
            batch_end = batch_begin + size -1
            left = left - size
            batches.append((batch_begin,batch_end))
        return batches

    def bulk_common_(self, op, objname, soql_or_csv, max_batch_size,
                     upsert_external_field=None):
        batch_res = []
        try:
            # create job
            self.bulk.job_create(op, objname, upsert_external_field)
            
            batch_ids = []
            if type(soql_or_csv) is list:
                if len(soql_or_csv):
                    header = soql_or_csv[0]
                    lines_count = len(soql_or_csv) - 1
                    batch_idx = 0
                    batch_ranges = SfBulkConnector.batch_ranges(lines_count,
                                                                max_batch_size)
                    for batch_range in batch_ranges:
                        batch_data = [header]
                        batch_data.extend(
                            soql_or_csv[batch_range[0]+1:
                                        batch_range[1]+2])
                        # create batch
                        batch_id = self.bulk.batch_create(''.join(batch_data))
                        batch_ids.append(batch_id)
            else:
                batch_id = self.bulk.batch_create(soql_or_csv)
                batch_ids.append(batch_id)
        
            # wait until job is completed
            while (not self.bulk.job_is_completed()):
                sleep(5)

            for batch_id in batch_ids:
                self.handle_batch_error(batch_id)
                one_res = self.bulk.batch_result()[batch_id]
                if not batch_res:
                    batch_res.extend(one_res)
                elif len(one_res):
                    batch_res.extend(one_res[1:])

            self.bulk.job_close()
            return batch_res
        except:
            if self.bulk.jobinfo and self.bulk.jobinfo.id:
                self.bulk.job_close()
            raise

    def bulk_insert(self, objname, csv_data, max_batch_size):
        res = self.bulk_common_('insert', objname, csv_data,
                                max_batch_size)
        self.handle_op_returning_ids('insert', res)
        return res

    def bulk_upsert(self, objname, csv_data, max_batch_size,
                    upsert_external_field):
        res = self.bulk_common_('upsert', objname, csv_data,
                                upsert_external_field)
        self.handle_op_returning_ids('upsert', res)
        return res

    def bulk_delete(self, objname, csv_data, max_batch_size):
        res = self.bulk_common_('delete', objname, csv_data,
                                max_batch_size)
        self.handle_op_returning_ids('delete', res)
        return  res

    def bulk_update(self, objname, csv_data, max_batch_size):
        res = self.bulk_common_('update', objname, csv_data,
                                max_batch_size)
        self.handle_op_returning_ids('update', res)
        return res

    def bulk_load(self, objname, soql):
        res = self.bulk_common_('query', objname, soql, None)
        return res

