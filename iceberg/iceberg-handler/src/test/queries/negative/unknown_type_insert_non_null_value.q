CREATE EXTERNAL TABLE unknown_test_non_null (
  id INT,
  placeholder UNKNOWN
) STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO unknown_test_non_null VALUES (1, 1);
