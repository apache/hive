CREATE EXTERNAL TABLE unknown_test_v2 (
  id INT,
  placeholder UNKNOWN
) STORED BY ICEBERG tblproperties('format-version'='2');
