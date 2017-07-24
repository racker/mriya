-- this should fail as wrong column names
SELECT '234о54658690938755' as MonsterRecordId, '098734658690938726' as MergerRecordId \
=> csv:Merge_dst_Account \
=> dst:merge:Account:200:Merge_dst_Account_res_ids
