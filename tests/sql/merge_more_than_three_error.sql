CREATE TABLE formerge(MasterRecordId, MergeRecordId);\
INSERT INTO formerge values('098734658690938726', '256345634690938726');\
INSERT INTO formerge values('098734658690938726', '256345634690938356');\
INSERT INTO formerge values('098734658690938726', '224352352546666356');\
SELECT * FROM formerge \
=> csv:Merge_dst_Account \
=> dst:merge:Account:200:Merge_dst_Account_res_ids
