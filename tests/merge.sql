SELECT MasterRecordId, MergeRecordId FROM csv.mergedata \
=> csv:Merge_dst_Account \
=> dst:merge:Account:200:Merge_dst_Account_res_ids
