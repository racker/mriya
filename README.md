# mriya
The hybrid query engine that combines Salesforce bulk queries with
Sqlite3 queries. It supports batches of SQL queries wrapped into
specific syntax. Salesforce operations supported:
query,insert,update,delete. The results of queries are saving into
csv files that are readable by sqlite3. So any data transformation can
be done locally by sqlite and then submitted to salesforce table. One
thing to note, that Salesforce bulk query syntax is little bit
different from SOAP query syntax.

Syntax in examples:
SELECT 'field1,field2' => var:FIELDS
SELECT something from src.SalesforceTable => csv:Opportunity1
SELECT Id,{fields} from dst.SalesforceTable => csv:Opportunity2
SELECT Id, {fields} FROM csv.Opportunity2 => csv:Opportunity_something_update => dst:update:Opportunity:10000:Update_Result_Ids


1. 
export PYTHONPATH=pybulk/sfbulk/:.

2. config file
2.1. Format
--- config file sections / options:
[src]
...
[dst]
consumer_key = 
consumer_secret = 
username = 
password = 
host_prefix = 
2.2. Specify [test] section to run tests

3. run test