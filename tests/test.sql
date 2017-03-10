SELECT "SELECT 'pen' as field1, 'pineapple' as field2, 'apple' as field3, 'pen' as field4" => var:QUERY => const:

=> macro:macro_test \
   :RES_TABLE_NAME:test_fields_table

SELECT * from csv.ints10000 => csv:aggregation_test

SELECT * FROM csv.test_fields_table => var:info:publish
