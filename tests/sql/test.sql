SELECT "SELECT 'pen' as field1, 'pineapple' as field2, 'apple' as field3, 'pen' as field4" => var:QUERY => const:

=> macro:macro_test \
   :RES_TABLE_NAME:test_fields_table

SELECT * from csv.ints10000 => csv:aggregation_test

-- put following line just to test some branches in graph.py
SELECT fake FROM src.fake => csv:aggregation_test:cache
SELECT fake FROM dst.fake2 => csv:aggregation_test:cache

SELECT * FROM csv.test_fields_table => var:info:publish
