
__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from mriya import bulk_data
from sfbulk import Bulk
from logging import getLogger
from time import sleep
from mriya.base_connector import BaseBulkConnector
from mriya.log import loginit, STDERR, STDOUT, LOG

class SfBulkConnector(BaseBulkConnector):

    def __init__(self, conn_param):
        super(SfBulkConnector, self).__init__(conn_param)
        #loginit(__name__)
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
                try:
                    errmes = info['stateMessage']
                except:
                    errmes = 'SF Error occured, failed to get description'
                getLogger(STDERR).error('%s bachFailed %s',
                                        batch_id, errmes)

    def handle_op_returning_ids(self, opname, objname, res):
        result_ids = bulk_data.parse_batch_res_data(res)
        id_idx = result_ids.fields.index('Id')
        success_idx = result_ids.fields.index('Success')
        error_idx = result_ids.fields.index('Error')
        max_err_count_output = 100
        for item in result_ids.rows:
            if item[success_idx] != 'true':
                max_err_count_output = max_err_count_output -1
                if max_err_count_output > 0:
                    getLogger(STDERR).error('Batch %s-%s: Id=%s, error:%s',
                                            opname, objname,
                                            item[id_idx],
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

    def dispatch_batch(self, batch_range, whole_data):
        header = whole_data[0]
        batch_data = [header]
        one_batch_data = whole_data[batch_range[0]+1:
                                    batch_range[1]+2]
        for one_line in one_batch_data:
            batch_data.append(
                bulk_data.prepare_sf_data_to_send(one_line))
        # create batch
        #print "before", soql_or_csv, "after", batch_data
        batch_id = self.bulk.batch_create(''.join(batch_data))
        return batch_id
        

    def batch_result(self, batch_id, batch_data_is_exist):
        self.handle_batch_error(batch_id)
        one_res = self.bulk.batch_result()[batch_id]
        # get rid from empty line, add this at the end
        if one_res[-1] == '':
            one_res = one_res[:-1]
        #get res data
        if not batch_data_is_exist:
            return one_res
        elif len(one_res):
            return one_res[1:]
        else:
            return []

    def bulk_seq_common_(self, op, objname, soql_or_csv, max_batch_size,
                         upsert_external_field=None):
        batch_res = []
        try:
            # create job
            self.bulk.job_create(op, objname, upsert_external_field)
            
            lines_count = -1
            if type(soql_or_csv) is list:
                if len(soql_or_csv) > 1:
                    lines_count = len(soql_or_csv) - 1
                    batch_ranges = SfBulkConnector.batch_ranges(lines_count,
                                                                max_batch_size)
                    getLogger(LOG).info("Lines: %d, max_batch_size: %s seq batches cnt %d",
                                        lines_count, str(max_batch_size), len(batch_ranges))

                    for batch_range in batch_ranges:
                        batch_id = self.dispatch_batch(batch_range, soql_or_csv)
                        # wait until job is completed
                        while (not self.bulk.job_is_completed()):
                            sleep(5)
                        batch_res.extend(self.batch_result(batch_id,
                                                           len(batch_res)))
            else: #query
                raise Exception('bulk_common_ must be used for op:query')

            # last salesforce result line is always empty
            if batch_res[-1] != '':
                batch_res.append('')
            self.bulk.job_close()
            return batch_res
        except:
            if self.bulk.jobinfo and self.bulk.jobinfo.id:
                self.bulk.job_close()
            raise


    def bulk_common_(self, op, objname, soql_or_csv, max_batch_size,
                     upsert_external_field=None):
        batch_res = []
        try:
            # create job
            self.bulk.job_create(op, objname, upsert_external_field)
            
            batch_ids = []
            lines_count = -1
            if type(soql_or_csv) is list:
                if len(soql_or_csv) > 1:
                    lines_count = len(soql_or_csv) - 1
                    batch_ranges = SfBulkConnector.batch_ranges(lines_count,
                                                                max_batch_size)
                    for batch_range in batch_ranges:
                        batch_id = self.dispatch_batch(batch_range, soql_or_csv)
                        batch_ids.append(batch_id)
            else: #query
                batch_id = self.bulk.batch_create(soql_or_csv)
                batch_ids.append(batch_id)
        
            getLogger(LOG).info("Lines: %d, max_batch_size: %s batches %s",
                                     lines_count, str(max_batch_size), batch_ids)
            # wait until job is completed
            while (not self.bulk.job_is_completed()):
                sleep(5)

            for batch_id in batch_ids:
                batch_res.extend(self.batch_result(batch_id, len(batch_res)))
            # last salesforce result line is always empty
            if batch_res[-1] != '':
                batch_res.append('')
            self.bulk.job_close()
            return batch_res
        except:
            if self.bulk.jobinfo and self.bulk.jobinfo.id:
                self.bulk.job_close()
            raise

    def bulk_common(self, op, objname, soql_or_csv, max_batch_size,
                    sequential, upsert_external_field=None):
        if type(soql_or_csv) is list and sequential:
            return self.bulk_seq_common_(
                op, objname, soql_or_csv, max_batch_size,
                upsert_external_field)
        else:
            return self.bulk_common_(
                op, objname, soql_or_csv, max_batch_size,
                upsert_external_field)

    def bulk_insert(self, objname, csv_data, max_batch_size, seq):
        res = self.bulk_common('insert', objname, csv_data,
                                max_batch_size, seq)
        self.handle_op_returning_ids('insert', objname, res)
        return res

    def bulk_upsert(self, objname, csv_data, max_batch_size, seq,
                    upsert_external_field):
        # not supported 
        res = self.bulk_common('upsert', objname, csv_data,
                                upsert_external_field)
        self.handle_op_returning_ids('upsert', objname, res)
        return res

    def bulk_delete(self, objname, csv_data, max_batch_size, seq):
        res = self.bulk_common('delete', objname, csv_data,
                                max_batch_size, seq)
        self.handle_op_returning_ids('delete', objname, res)
        return  res

    def bulk_update(self, objname, csv_data, max_batch_size, seq):
        res = self.bulk_common('update', objname, csv_data,
                                max_batch_size, seq)
        self.handle_op_returning_ids('update', objname, res)
        return res

    def bulk_load(self, objname, soql):
        res = self.bulk_common('query', objname, soql, None, None)
        return res

