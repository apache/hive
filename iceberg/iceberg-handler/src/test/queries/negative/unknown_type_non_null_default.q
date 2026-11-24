CREATE EXTERNAL TABLE unknown_test_default (
  id INT,
  placeholder UNKNOWN DEFAULT 'abc'
) STORED BY ICEBERG tblproperties('format-version'='3');
