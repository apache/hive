--! qt:sysdb
--! qt:queryhistory
--! qt:transactional

CREATE TABLE test_part(id int) PARTITIONED BY(dt string) STORED AS ORC TBLPROPERTIES ('transactional'='true');

--BASIC QUERY, fetch
set hive.fetch.task.conversion=more;
SELECT * FROM test_part;

--TEZ
set hive.fetch.task.conversion=none;
SELECT * FROM test_part;

--ACID, DML
INSERT INTO test_part VALUES (1, '1');

--STATS
ANALYZE TABLE test_part COMPUTE STATISTICS;
ANALYZE TABLE test_part COMPUTE STATISTICS FOR COLUMNS;

--EXPLAIN
EXPLAIN SELECT * FROM test_part;
EXPLAIN INSERT INTO test_part VALUES (1, '1');

--SCHEMA/DATA manipulation
ALTER TABLE test_part ADD PARTITION (dt='3');

UPDATE test_part SET id = 2 WHERE dt = '1';
DELETE FROM test_part WHERE dt = '1';

CREATE TABLE test_part2(id int) PARTITIONED BY(dt string) STORED AS ORC TBLPROPERTIES ('transactional'='true');
INSERT OVERWRITE TABLE test_part2 SELECT * FROM test_part;

MERGE INTO test_part2 AS target USING test_part AS source ON source.id = target.id
WHEN MATCHED THEN UPDATE SET id=3
WHEN NOT MATCHED THEN INSERT VALUES (3, '4');
TRUNCATE TABLE test_part2;

--ICEBERG
CREATE EXTERNAL TABLE test_iceberg(id int) PARTITIONED BY(dt string) STORED BY ICEBERG;
INSERT INTO test_iceberg VALUES (1, '1');
TRUNCATE TABLE test_iceberg;

--CREATE DATABASE/TABLE
CREATE DATABASE query_history_qtest_db;
CREATE TABLE query_history_qtest_db.test_table(id int);

--SHOW DATABASE/TABLE
SHOW DATABASES;
USE query_history_qtest_db;
SHOW TABLES;
USE default;

--WITH clause
with test_part_with_alias as (select * from test_part) select * from test_part_with_alias;

-- any operation on query_history table might involve further record changes on query_history table,
-- so let's disable the feature while to reduce noise while checking the contents
set hive.query.history.enabled=false;
set hive.compute.query.using.stats=false;

-- below are some query history queries: only those fields were queries that are constant across test runs,
-- so duration-like fields are not added here for instance, for full schema, please refer to QueryHistorySchema

-- basic query data
SELECT concat(substr(sql, 0, 30), "..."), cluster_id, session_type, cluster_user, end_user, db_name, query_type, operation
FROM sys.query_history ORDER BY start_time;

-- environment related data
SELECT concat(substr(sql, 0, 30), "..."), server_address, server_port, client_address
FROM sys.query_history ORDER BY start_time;

-- misc fields
SELECT concat(substr(sql, 0, 30), "..."), failure_reason, num_rows_fetched, configuration_options_changed
FROM sys.query_history ORDER BY start_time;
