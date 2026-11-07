CREATE EXTERNAL TABLE unknown_test_struct (
  id INT,
  s STRUCT<u:UNKNOWN>
) STORED BY ICEBERG tblproperties('format-version'='3');
