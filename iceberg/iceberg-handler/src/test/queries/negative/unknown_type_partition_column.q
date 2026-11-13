CREATE EXTERNAL TABLE unknown_test_partition (
  id INT,
  placeholder UNKNOWN
) PARTITIONED BY spec (placeholder)
STORED BY ICEBERG tblproperties('format-version'='3');
