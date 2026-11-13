CREATE EXTERNAL TABLE unknown_test_array (
  id INT,
  l ARRAY<UNKNOWN>
) STORED BY ICEBERG tblproperties('format-version'='3');
