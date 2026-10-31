CREATE EXTERNAL TABLE unknown_test_promote_complex (
  id INT,
  placeholder UNKNOWN
) STORED BY ICEBERG tblproperties('format-version'='3');

ALTER TABLE unknown_test_promote_complex CHANGE COLUMN placeholder placeholder ARRAY<INT>;
