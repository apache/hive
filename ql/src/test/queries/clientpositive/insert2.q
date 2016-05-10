set hive.explain.user=false;
set fs.defaultFS=file:///;

-- SORT_QUERY_RESULTS

USE default;
CREATE DATABASE db1;
CREATE TABLE db1.result(col1 STRING);
INSERT INTO TABLE db1.result SELECT 'db1_insert1' FROM src LIMIT 1;
INSERT INTO TABLE db1.result SELECT 'db1_insert1' FROM src LIMIT 1;
SELECT * FROM db1.result;

reset fs.defaultFS;
