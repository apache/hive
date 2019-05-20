--! qt:dataset:src
-- Test that CREATE TABLE LIKE commands can take explicit table properties

CREATE TABLE test_table_n15 LIKE src TBLPROPERTIES('key'='value');

DESC FORMATTED test_table_n15;

set hive.table.parameters.default=key1=value1;

--Test that CREATE TABLE LIKE commands can take default table properties

CREATE TABLE test_table1_n18 LIKE src;

DESC FORMATTED test_table1_n18;

-- Test that CREATE TABLE LIKE commands can take default and explicit table properties

CREATE TABLE test_table2_n17 LIKE src TBLPROPERTIES('key2' = 'value2');

DESC FORMATTED test_table2_n17;

set hive.ddl.createtablelike.properties.whitelist=key2;

-- Test that properties inherited are overwritten by explicitly set ones

CREATE TABLE test_table3_n9 LIKE test_table2_n17 TBLPROPERTIES('key2' = 'value3');

DESC FORMATTED test_table3_n9;

--Test that CREATE TALBE LIKE on a view can take explicit table properties

CREATE VIEW test_view (key, value) AS SELECT * FROM src;

CREATE TABLE test_table4_n2 LIKE test_view TBLPROPERTIES('key'='value');

DESC FORMATTED test_table4_n2;
