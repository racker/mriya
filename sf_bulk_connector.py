
__author__ = "Yaroslav Litvinov"
__copyright__ = "Copyright 2016, Rackspace Inc."
__email__ = "yaroslav.litvinov@rackspace.com"

from base_connector import BaseBulkConnector
from sfbulk import Bulk
from logging import getLogger, DEBUG
from time import sleep

class SfBulkConnector(BaseBulkConnector):

    def __init__(self, conn_param):
        super(SfBulkConnector, self).__init__(conn_param)
        # initialize bulk
        self.bulk = Bulk(self.instance_url)
        self.bulk.login(username=self.conn_param.username,
                        password=self.conn_param.password,
                        security_token=self.conn_param.token,
                        #sf_version=38.0,
                        sandbox=True)

    def handle_batch_exception(self, batch_id):
        """ Handle critical bulk error, when no records processed """
        batch_res = None
        if batch_id in self.bulk.jobinfo.batch:
            info = self.bulk.jobinfo.batch[batch_id]
            if info['state'] != 'Completed':
                raise Exception('bachFailed %s' % batch_id,
                                info['stateMessage'])
        else:
            assert(0)

    def bulk_insert(self, objname, csv_data):
        # create job
        self.bulk.job_create('insert', objname)
        
        # create batch
        self.bulk.batch_create(csv_data)
        
        # wait until job is completed
        while (not self.bulk.job_is_completed()):
            sleep(5)
        
        res = self.bulk.batch_result()

        # close job
        self.bulk.job_close()
        
        return res

    def bulk_load(self, objname, soql):
        # create job
        self.bulk.job_create('query', objname)
        
        # create batch
        batch_id = self.bulk.batch_create(soql)
        
        # wait until job is completed
        while (not self.bulk.job_is_completed()):
            sleep(5)
        
        self.handle_batch_exception(batch_id)
        batch_res = self.bulk.batch_result()[batch_id]
        # close job
        self.bulk.job_close()

        return batch_res

