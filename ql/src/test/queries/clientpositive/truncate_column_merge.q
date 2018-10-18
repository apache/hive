--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- Tests truncating a column from a table with multiple files, then merging those files

CREATE TABLE test_tab_n2 (key STRING, value STRING) STORED AS RCFILE;

INSERT OVERWRITE TABLE test_tab_n2 SELECT * FROM src tablesample (5 rows);

INSERT INTO TABLE test_tab_n2 SELECT * FROM src tablesample (5 rows);

-- The value should be 2 indicating the table has 2 files
SELECT COUNT(DISTINCT INPUT__FILE__NAME) FROM test_tab_n2;

TRUNCATE TABLE test_tab_n2 COLUMNS (key);

ALTER TABLE test_tab_n2 CONCATENATE;

-- The first column (key) should be null for all 10 rows
SELECT * FROM test_tab_n2 ORDER BY value;

-- The value should be 1 indicating the table has 1 file
SELECT COUNT(DISTINCT INPUT__FILE__NAME) FROM test_tab_n2;
