CREATE EXTERNAL TABLE unknown_test_add_column (
  id INT
) STORED BY ICEBERG tblproperties('format-version'='3');

ALTER TABLE unknown_test_add_column ADD COLUMNS (l ARRAY<UNKNOWN>);
