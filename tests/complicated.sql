-- This is a test code which completely mocked by tests, every change in this file
-- should be reflected by change in mock_job_controller mock related to
-- test_job_reader.test_job_controller

SELECT Id,Account_Birthday__c,Name,Alexa__c FROM src.Account LIMIT 1; \
=> csv:some_data

SELECT Id,Account_Birthday__c,Name,Alexa__c FROM src.Account LIMIT 1; \
=> csv:some_data:cache

SELECT 'csv.some_data' => var:CSVTABLE => const:

SELECT Id from {CSVTABLE} LIMIT 1; => var:id_test

-- this statement is just for testing coverage
SELECT a.Id, b.Id FROM csv.some_data a \
INNER JOIN {CSVTABLE} b ON a.Id = b.Id and b.Id =777 \
=> csv:nonsencejoin
---------------------------------------------

SELECT Account_Birthday__c,Name,Alexa__c FROM csv.some_data; \
=> csv:some_data_staging => dst:insert:Account:1:newids => type:sequential

UPDATE csv.some_data SET Account_Birthday__c=null, Name='test1234567'; \
SELECT Id,Account_Birthday__c,Name,Alexa__c FROM csv.some_data \
WHERE Id = '{id_test}' \
=> csv:some_data_staging => dst:update:Account:1:res_ids

SELECT '{id_test}' as Id,Alexa__c FROM csv.test_csv => csv:some_data_staging2 => \
dst:update:Account:1:res_ids => type:parallel

SELECT Alexa__c FROM dst.Account WHERE Id = '{id_test}' => csv:test_csv_2

-- this line is only improving coverage in graph.py
SELECT '{id_test}' as Id,Alexa__c FROM csv.test_csv => csv:some_data_staging2 => \
src:update:Account:1:res_ids

-- covering batch related code in graph.py, actualy this code do nothing
-- it's only printing BATCH_VAR value in a loop
SELECT i as batch_value FROM csv.ints10000 LIMIT 10 \
=> csv:batch_idx => batch_begin:batch_value:BATCH_VAR

SELECT '{BATCH_VAR}' => var:info:publish

=> batch_end:BATCH_VAR
