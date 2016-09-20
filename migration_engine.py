__author__ = 'Volodymyr Varchuk'


import logging
import tempfile
from project_utils import printProgress, success_records_check


class MigrationEngine:
    def __init__(self, src_connector, dst_connector, mapping_parser):
        self.src = src_connector
        self.dst = dst_connector
        self.mapping = mapping_parser
        self.temp_dir = tempfile.mkdtemp(prefix='sfdm_')
        self.data=[]
        logging.info('Migration engine initiate with temporary directory {0}'.format(self.temp_dir))


    def dst_update(self):
        data = self.src_extract()
        self.upd_dst_mapping = self.mapping.dst_update_relations()
        new_data = []
        # printProgress(0, len(self.raw_data), prefix='Data processed', suffix='Complete', decimals=1, barLength=100)
        logging.info('start processing')
        for row in data:
            add_row = {}
            for key, val in self.upd_dst_mapping.iteritems():
                if key == 'type':
                    add_row[key] = self.mapping.destination_object
                else:
                    add_row[key] = row[val]

            # if len(self.new_data) % 100:
            #     printProgress(len(self.new_data), len(self.raw_data), prefix='Data processed', suffix='Complete', decimals=1, barLength=100)
            new_data.append(add_row)
        logging.info('done processing')
        print('{0} rows will be updated on destination'.format(len(new_data)))
        # make changes to how it works in TEST environment
        for row_new_data in new_data:
            # just for relations ID of already existant object in table Owner
            row_new_data['Company_Name__c'] = '0013B000006M6zSQAS'
        print(new_data)
        self.dst.bulk_insert(self.mapping.destination_object, new_data)
        migrated_old_ids = []
        for row in data:
            migrated_old_ids.append(row['Id'])
        return migrated_old_ids


    def src_update(self, migrated_ids=None):
        #
        new_created = self.dst.bulk_load(self.mapping.destination_object, self.mapping.dst_get_soql_created_records(migrated_ids))
        src_update_data = []
        for row_created in new_created:
            row_for_update = {}
            row_for_update[self.mapping.get_src_old_id_column()] = row_created['Id']
            row_for_update['Id'] = row_created[self.mapping.src_update_mapping['Id']]
            src_update_data.append(row_for_update)
        if len(src_update_data) > 0:
            print('{0} rows will be updated on source'.format(len(src_update_data)))
            # chunked
            # self.src_update_result = self.src.svc.update(src_update_data)
            self.src_update_result = self.src.bulk_update(self.mapping.source_object, src_update_data)
            # print('{0} rows updated successfully on source'.format(success_records_check(self.src_update_result)))
            return self.src_update_result
        else:
            print('nothing to update')
            return None


    def migrate(self):
        migrated_ids = self.dst_update()
        print(self.src_update(migrated_ids))



    def src_extract(self):
        logging.info('Begin extracting data to memroy')
        raw_data = self.src.bulk_load(self.mapping.source_object, self.mapping.get_src_soql())
        logging.info('Done extracting data to memroy')
        return raw_data


    def src_extract_bulk(self):
        self.soql = self.mapping.src_extract_soql_generator()
        # self.raw_data = self.src. .fetch_all_data(self.soql)




    def dst_extract(self):
        print('Prepare Destination')
