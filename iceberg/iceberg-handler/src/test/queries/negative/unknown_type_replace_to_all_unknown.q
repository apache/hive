CREATE EXTERNAL TABLE unknown_test_replace_to_unknown (
    id INT,
    u UNKNOWN
) STORED BY ICEBERG tblproperties('format-version'='3');

ALTER TABLE unknown_test_replace_to_unknown REPLACE COLUMNS (u UNKNOWN);