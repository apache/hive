CREATE EXTERNAL TABLE unknown_test_drop_to_unknown (
    id INT,
    u UNKNOWN
) STORED BY ICEBERG tblproperties('format-version'='3');

ALTER TABLE unknown_test_drop_to_unknown DROP COLUMN id;