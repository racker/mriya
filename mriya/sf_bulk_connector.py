
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
        batch = res[0]
        batch_res = res[1]
        result_ids = parse_batch_res_data(batch_res)
        id_idx = result_ids.fields.index('Id')
        success_idx = result_ids.fields.index('Success')
        error_idx = result_ids.fields.index('Error')
        for item in result_ids.rows:
            if item[success_idx] != 'true':
                getLogger(__name__).error('Batch %s %s: Id=%s, error:%s',
                                          batch, opname, item[id_idx],
                                          item[error_idx])

    def bulk_common_(self, op, objname, soql_or_csv, upsert_external_field=None):
        try:
            # create job
            self.bulk.job_create(op, objname, upsert_external_field)
        
            # create batch
            batch_id = self.bulk.batch_create(soql_or_csv)
        
            # wait until job is completed
            while (not self.bulk.job_is_completed()):
                sleep(5)

            self.handle_batch_error(batch_id)
            batch_res = self.bulk.batch_result()[batch_id]
            self.bulk.job_close()
            return (batch_id, batch_res)
        except:
            if self.bulk.jobinfo.id:
                self.bulk.job_close()
            raise

    def bulk_insert(self, objname, csv_data):
        res = self.bulk_common_('insert', objname, csv_data)
        self.handle_op_returning_ids('insert', res)
        return res[1]

    def bulk_upsert(self, objname, csv_data, upsert_external_field):
        res = self.bulk_common_('upsert', objname, csv_data,
                                upsert_external_field)
        self.handle_op_returning_ids('upsert', res)
        return res[1]


    def bulk_update(self, objname, csv_data):
        res = self.bulk_common_('update', objname, csv_data)
        self.handle_op_returning_ids('update', res)
        return res[1]

    def bulk_load(self, objname, soql):
        res = self.bulk_common_('query', objname, soql)
        return res[1]

