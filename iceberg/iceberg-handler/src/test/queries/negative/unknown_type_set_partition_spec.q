CREATE EXTERNAL TABLE unknown_test_set_spec (
  id INT,
  placeholder UNKNOWN
) STORED BY ICEBERG tblproperties('format-version'='3');

ALTER TABLE unknown_test_set_spec SET PARTITION SPEC (placeholder);
