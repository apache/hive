--! qt:dataset:src
set hive.exec.pre.hooks = org.apache.hadoop.hive.ql.hooks.EnforceReadOnlyDatabaseHook;

ALTER DATABASE default SET DBPROPERTIES('readonly' = 'true');

-- SET
set hive.exec.reducers.max = 1;

-- USE
use default;

-- SHOW
SHOW DATABASES;
SHOW CREATE DATABASE default;
SHOW CREATE TABLE src;

-- DESC
DESC DATABASE default;

-- DESCRIBE
DESCRIBE src;

-- EXPLAIN
EXPLAIN SELECT * FROM src LIMIT 1;

-- SELECT
SELECT * FROM src LIMIT 1;
