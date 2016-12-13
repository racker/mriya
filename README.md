# mriya
The hybrid query engine that combines Salesforce bulk queries with
Sqlite3 queries. It supports batches of SQL queries wrapped into
specific syntax. Salesforce operations supported:
query,insert,update,delete. The results of queries are saving into
csv files that are readable by sqlite3. So any data transformation can
be done locally by sqlite and then submitted to salesforce table. One
thing to note, that Salesforce bulk query syntax is little bit
different from SOAP query syntax.

* Install.<br>Requirements and export PYTHONPATH:
```
pip install -r requirements.txt
export PYTHONPATH=pybulk/sfbulk/:.
```
* Config file.<br>
Use sample-config.ini as a base for your config files.<br>
Specify `[dst]` and `[src]` sections for dst/src endpoints.
```
[dst]
consumer_key = 
consumer_secret = 
username = 
password = 
host_prefix = 
```

* Tests<br>
`[test]` section must be specified in order to run tests. Be sure to provide credentials to non production instance as test itself is adding and removing records from salesforce Account object.

* Troubleshooting<br>
```AttributeError: ConfigParser instance has no attribute 'read_file'```<br>
If you getting that error, be sure to install and use configparser==3.5.0.

* Syntax
Use sqlite3 sql syntax while querying local csv tables, and SF bulk query language when running SF bulk queries. `csv` endpoint means local sqlite3 table and `src` / `dst` endpoints mean remote SF table. When issuing request endpoint's str will be removed from query.<br>
`SELECT 1 from csv.table;` transformates into local sqlite3 query: `SELECT 1 from table;`<br>
`SELECT Id from src.table;` transformates into SF query: `SELECT Id from table;`

* Variables via cmdline.<br>
Add variable/s from cmd line:<br>
```mriya_dmt.py --var MY_USER_ID 00561000001znSnAAI --var ID2 12345```

* Syntax in examples.<br>

Comment is started as `--` at the beginning of line
```sql
-- comment section. 
```

assign value to variable
```sql
SELECT 'field1,field2' => var:FIELDS
```

variable can only be used in query section, and can't be used after `=>`
```sql
SELECT {FIELDS} FROM csv.table => csv:test
```

Publish info at stdout. assign value to variable and put it to stdout
```sql
SELECT 'field1,field2' => var:FIELDS:publish
```

Prevent removing endpoints from value during substitution. Use `=> const:` to mark variable as non changeable during substitution. if `const:` is ommited then endpoint will be always removed. It's useful to use `const:` when using variable during query construction:
```sql
SELECT " 'str' as field1, CAST(field2 as INTEGER) FROM csv.table" \
=> var:FIELDS \
=> const:
SELECT {FIELDS} => csv:newtable
```

Issue bulk request to SF endpoint denoted as `src` and query table `SalesforceTable`, then save result into `csv` file `Opportunity1`
```sql
SELECT something from src.SalesforceTable => csv:Opportunity1
```

Construct query using variable's value and issue request it to SF instance at `dst`, save result into `csv` file `Opportunity2`
```sql
SELECT Id,{fields} from dst.SalesforceTable => csv:Opportunity2
```

Use `\` to make long single queries fancier by writing them in mulitiple lines. If '\' symbol is located at the end of a row it's will be concatenated with next row. **Any** single query is oneliner.
Following example explanation:<br>
Select data from local `csv` table `Opportunity2` and save it to another `csv` table `Opportunity_something_update` and then submit update bulk request using data from `Opportunity_something_update` table to `dst` SF instance. Save processed list of ids returned by SF into csv table `Update_Result_Ids`. All the content of table will be submitted as list of batches with max batch size = 10000. Batches will be executed one by one as `type:sequential` was specified. Batches would run in parallel if type `type:parallel` or nothing specified [type:parallel is by default]
```sql
SELECT Id, {fields} FROM csv.Opportunity2 \
=> csv:Opportunity_something_update \
=> dst:update:Opportunity:10000:Update_Result_Ids \
=> type:sequential
```

insert, update, delete SF batches are supported.<br>
Examples:
```sql
SELECT f1,f2 FROM csv.foo => csv:export => dst:insert:10000:list_of_processed_ids_errors
SELECT f1,f2 FROM csv.foo => csv:export => dst:delete:10000:list_of_processed_ids_errors
SELECT f1,f2 FROM csv.foo => csv:export => src:update:10000:list_of_processed_ids_errors
```

Macroses<br>
macro will be substituted by its value read from corresponding file.<br>
Macro file `macro_test` is supposed to be existed in scripts folder. All previously defined variables can be used inside of macros. Macro param value should not contains spaces. Many macro params may be specified(params set is different for diferent macroses).
```sql
SELECT 'hello' => var:VAR1 \
=> macro:macro_test \
   :PARAM1:param_value_no_spaces \
   :PARAM2:some_table
```

example of macro file:
```
-- {PARAM1}, {PARAM2} will be substituted by param value param_value_no_spaces
-- {VAR1} will be substituted by var value
SELECT {PARAM1}, '{VAR1}' as hello, ID FROM csv.table => csv:{PARAM2}_some_string
-- resulted macro will be transformated into:
-- SELECT param_value_no_spaces, 'hello' as hello, ID FROM csv.table => csv:some_table_some_string
```

Use following construction to run some code in loop
```sql
-- SELECT_BATCH_IDX - variable having different value on different iterations
-- Query located before '=> batch_begin' is getting a list of values for loop
SELECT CAST(i as INTEGER) as idx FROM csv.ints10000 LIMIT 10 \
=> batch_begin:idx:SELECT_BATCH_IDX
   -- run following code 10 times
   SELECT '{SELECT_BATCH_IDX}' => var:info:publish
=> batch_end:SELECT_BATCH_IDX
```

