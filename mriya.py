__author__ = 'Volodymyr Varchuk'


import test_data_generator
import data_connector
import migration_engine
import mapping_parser
import json
import project_utils
from configparser import ConfigParser
from data_connector import get_conn_param

config_file = 'config.ini'
mapping_file = 'mapping_test_01.json'

config = ConfigParser()
with open(config_file, 'r') as conf_file:
   config.read_file(conf_file)

mapping_obj = mapping_parser.MappingParser(json.load(open(mapping_file, 'r'))[0])

print(mapping_obj.get_dst_soql_cond(['1111', '2222']))
print(mapping_obj.get_src_soql())

# print(mapping_obj.dst_get_soql_created_records(['11111', '22222']))
# exit(0)

#rackspace--uat.cs13.my.salesforce.com
src_param = get_conn_param(config['src'])
dst_param = get_conn_param(config['dst'])

src_delete = data_connector.SFBeatboxConnector(src_param)
dst_delete = data_connector.SFBeatboxConnector(dst_param)
# dst = data_connector.SFBeatboxConnector(dst_param)
# res_old = src.svc.query('Select id, Name From Account LIMIT 1')
# res_new = dst.svc.query('Select id, Name From Account LIMIT 1')
# print(res_old)
# print(res_new)

src = data_connector.RESTConnector(src_param)
dst = data_connector.RESTConnector(dst_param)


# src.bulkload(mapping_obj.soql, mapping_obj.get_src_columns())
# dst.bulkload()


# exit(0)

src_data = src_delete.fetch_all_data ('SELECT ID FROM Account')
print(src_data)
src_ids_list = [old_id['Id'] for old_id in src_data]
print(src_ids_list)

exit(0)

print('Preparing source and destination')
if len(src_ids_list) > 0:
   # src_del_res = src.svc.delete(src_ids_list)
   src_del_res = src_delete.chunked_delete(src_ids_list)
   print('{0} records deleted from source'.format(project_utils.success_records_check(src_del_res)))
   rows_deleted = 0
   while len(src_ids_list) >= rows_deleted:
       end_chunk = 200 if len(src_ids_list) > rows_deleted + 200 else len(src_ids_list)
       where_condition = ' or '.join(["Old_Record_Id__c='{0}'".format(old_id) for old_id in src_ids_list[rows_deleted:end_chunk]])
       if where_condition == '':
           break
       dst_soql = 'SELECT Old_Record_Id__c, ID FROM Accounts__c WHERE ' + where_condition
       dst_data = dst_delete.svc.query(dst_soql)
       dst_records = dst_data['records']
       if len(dst_records) > 0:
           # delete_result = dst.svc.delete([new_id['Id'] for new_id in dst_records])
           delete_result = dst_delete.chunked_delete([new_id['Id'] for new_id in dst_records])
           print('{0} records deleted from destination'.format(project_utils.success_records_check(delete_result)))
       else:
           print('Skip destination deletion')
       rows_deleted = end_chunk

print('Generating test data for source')
data_generator = test_data_generator.DataGenerator(test_data_generator.data_struct, test_data_generator.defaults,test_data_generator.prefixes)
new_data = data_generator.gen_data(4, 'Account')
src.bulk_insert ('Account', new_data)
r_soql = mapping_obj.get_src_soql()
uploaded = src.bulk_load(mapping_obj.source_object, r_soql)
print(uploaded)
print('{0} records with test data inserted to source'.format(len(uploaded)))

mig_engine = migration_engine.MigrationEngine(src,dst,mapping_obj)

mig_engine.migrate()


# dst_update_result = mig_engine.dst_update()
# src_update_result = mig_engine.src_update()

