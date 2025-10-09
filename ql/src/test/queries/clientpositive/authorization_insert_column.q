set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set test.hive.authz.sstd.validator.outputPrivObjs=true;
set test.hive.authz.sstd.validator.bypassObjTypes=DATABASE;
set hive.test.authz.sstd.hs2.mode=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.support.concurrency=true;
set user.name=testuser;


CREATE DATABASE IF NOT EXISTS test_insert_col_db;
USE test_insert_col_db;

-- Negative Case 1: Basic Table with Restricted Column
CREATE TABLE test_basic_table (
  id INT,
  name STRING,
  age INT
);

SHOW DATABASES LIKE 'test_insert_col_db';
SHOW TABLES IN test_insert_col_db;
EXPLAIN INSERT INTO test_insert_col_db.test_basic_table (id, name, age) VALUES (1, 'Dummy', 24);
DROP TABLE test_basic_table;

-- Negative Case 2: Partitioned Table with Restricted Column
CREATE TABLE test_partitioned_table (
  id INT,
  name STRING,
  age INT
)
PARTITIONED BY (year INT);

ALTER TABLE test_partitioned_table ADD PARTITION (year=2024);

SHOW DATABASES LIKE 'test_insert_col_db';
SHOW TABLES IN test_insert_col_db;
EXPLAIN INSERT INTO test_insert_col_db.test_partitioned_table (id, name, age) VALUES (2, 'Dummy2', 25);
DROP TABLE test_partitioned_table;

-- Negative Case 3: ACID Table with Restricted Column
CREATE TABLE test_acid_table (
  id INT,
  name STRING,
  age INT
)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');


SHOW DATABASES LIKE 'test_insert_col_db';
SHOW TABLES IN test_insert_col_db;
EXPLAIN INSERT INTO test_insert_col_db.test_acid_table (id, name, age) VALUES (3, 'Dummy3', 26);
DROP TABLE test_acid_table;

-- Negative Case 4: Insert Without Specifying Column Names (Restricted Column)
CREATE TABLE test_restricted_column_table (
  id INT,
  name STRING,
  age INT
);

SHOW DATABASES LIKE 'test_insert_col_db';
SHOW TABLES IN test_insert_col_db;
EXPLAIN INSERT INTO test_insert_col_db.test_restricted_column_table VALUES (8, 'Dummy4', 55);
DROP TABLE test_restricted_column_table;

DROP DATABASE test_insert_col_db;
