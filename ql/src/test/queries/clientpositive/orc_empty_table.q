CREATE TABLE test_orc_empty_table_with_struct (struct_field STRUCT<int_field: INT>) STORED AS ORC;
SELECT count(*) FROM test_orc_empty_table_with_struct;

CREATE TABLE test_orc_empty_table_with_map (map_field MAP<STRING,STRING>) STORED AS ORC;
SELECT count(*) FROM test_orc_empty_table_with_map;

CREATE TABLE test_orc_empty_table_with_list (list_field ARRAY<INT>) STORED AS ORC;
SELECT count(*) FROM test_orc_empty_table_with_list;

CREATE TABLE test_orc_empty_table_with_union (union_field UNIONTYPE<int, double>) STORED AS ORC;
SELECT count(*) FROM test_orc_empty_table_with_union;
