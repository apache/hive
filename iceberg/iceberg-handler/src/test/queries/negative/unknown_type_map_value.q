CREATE EXTERNAL TABLE unknown_test_map (
  id INT,
  m MAP<STRING, UNKNOWN>
) STORED BY ICEBERG tblproperties('format-version'='3');
