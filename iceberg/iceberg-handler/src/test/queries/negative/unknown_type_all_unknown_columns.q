CREATE EXTERNAL TABLE unknown_test_all_unknown (
    u UNKNOWN
) STORED BY ICEBERG tblproperties('format-version'='3');