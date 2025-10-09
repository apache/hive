CREATE EXTERNAL TABLE variant_test_partition (
  id INT,
  data VARIANT
) PARTITIONED BY spec (data)
STORED BY ICEBERG tblproperties('format-version'='3');