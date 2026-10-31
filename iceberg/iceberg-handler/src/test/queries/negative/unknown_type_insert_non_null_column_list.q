CREATE EXTERNAL TABLE unknown_test_non_null_cols (
  id INT,
  placeholder UNKNOWN
) STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO unknown_test_non_null_cols (id, placeholder) VALUES (1, 'abc');
