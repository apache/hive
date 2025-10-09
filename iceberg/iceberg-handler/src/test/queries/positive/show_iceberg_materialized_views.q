--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE DATABASE test1;
CREATE DATABASE test2;

USE test1;
CREATE TABLE shtb_test1(KEY INT, VALUE STRING) PARTITIONED BY(ds STRING)
stored by iceberg stored as orc tblproperties ('format-version'='2');
CREATE TABLE shtb_test2(KEY INT, VALUE STRING) PARTITIONED BY(ds STRING)
STORED AS ORC TBLPROPERTIES ('transactional'='true');
CREATE MATERIALIZED VIEW shtb_test1_view1 DISABLE REWRITE stored by iceberg stored as orc tblproperties ('format-version'='1') AS
SELECT * FROM shtb_test1 where KEY > 1000 and KEY < 2000;
CREATE MATERIALIZED VIEW shtb_test1_view2 
stored by iceberg stored as orc TBLPROPERTIES ('rewriting.time.window' = '-1min', 'format-version'='1') AS
SELECT * FROM shtb_test1 where KEY > 100 and KEY < 200;
CREATE MATERIALIZED VIEW shtb_view_on_native
stored by iceberg stored as orc TBLPROPERTIES ('format-version'='1') AS
SELECT * FROM shtb_test2 where KEY > 100 and KEY < 200;
CREATE MATERIALIZED VIEW shtb_full_view2
stored by iceberg stored as orc tblproperties ('format-version'='1', 'rewriting.time.window' = '5min') AS
SELECT * FROM shtb_test1;
CREATE MATERIALIZED VIEW shtb_aggr_view1 stored by iceberg stored as orc tblproperties ('format-version'='1') AS
SELECT a.value, sum(a.key) FROM shtb_test1 a join shtb_test1 b on (a.key = b.key) group by a.value;
CREATE MATERIALIZED VIEW shtb_aggr_view2 stored by iceberg stored as orc tblproperties ('format-version'='1') AS
SELECT a.value, count(1), sum(a.key) FROM shtb_test1 a join shtb_test1 b on (a.key = b.key) group by a.value;


USE test2;
CREATE TABLE shtb_test1(KEY INT, VALUE STRING) PARTITIONED BY(ds STRING)
STORED AS TEXTFILE;
CREATE TABLE shtb_test2(KEY INT, VALUE STRING) PARTITIONED BY(ds STRING)
STORED AS TEXTFILE;
CREATE MATERIALIZED VIEW shtb_test1_view1 DISABLE REWRITE stored by iceberg stored as orc tblproperties ('format-version'='1') AS
SELECT * FROM shtb_test1 where KEY > 1000 and KEY < 2000;
CREATE MATERIALIZED VIEW shtb_test2_view2 DISABLE REWRITE stored by iceberg stored as orc tblproperties ('format-version'='1') AS
SELECT * FROM shtb_test2 where KEY > 100 and KEY < 200;

USE test1;
EXPLAIN SHOW MATERIALIZED VIEWS;
SHOW MATERIALIZED VIEWS;
EXPLAIN SHOW MATERIALIZED VIEWS '%test%';
SHOW MATERIALIZED VIEWS '%test%';
SHOW MATERIALIZED VIEWS '%view2';

USE test2;
SHOW MATERIALIZED VIEWS 'shtb_%';

-- SHOW MATERIALIZED VIEWS basic syntax tests
USE default;
EXPLAIN SHOW MATERIALIZED VIEWS FROM test1;
SHOW MATERIALIZED VIEWS FROM test1;
SHOW MATERIALIZED VIEWS FROM test2;
EXPLAIN SHOW MATERIALIZED VIEWS IN test1;
SHOW MATERIALIZED VIEWS IN test1;
SHOW MATERIALIZED VIEWS IN default;
EXPLAIN SHOW MATERIALIZED VIEWS IN test1 "shtb_test%";
SHOW MATERIALIZED VIEWS IN test1 "shtb_test%";
DESCRIBE FORMATTED test1.shtb_full_view2;
DESCRIBE FORMATTED test1.shtb_test1_view1;
DESCRIBE FORMATTED test1.shtb_test1_view2;
SHOW MATERIALIZED VIEWS IN test2 LIKE "nomatch";

-- SHOW MATERIALIZED VIEWS from a database with a name that requires escaping
CREATE DATABASE `database`;
USE `database`;
CREATE TABLE foo_n0(a INT)
stored by iceberg stored as orc tblproperties ('format-version'='2');
CREATE MATERIALIZED VIEW fooview
stored by iceberg stored as orc tblproperties ('format-version'='1', 'rewriting.time.window' = '0min') AS
SELECT * FROM foo_n0;
USE default;
SHOW MATERIALIZED VIEWS FROM `database` LIKE "fooview";
DESCRIBE FORMATTED `database`.`fooview`;

DROP MATERIALIZED VIEW fooview;
DROP TABLE foo_n0;

USE test1;
DROP MATERIALIZED VIEW shtb_test1_view1;
DROP MATERIALIZED VIEW shtb_test1_view2;
DROP MATERIALIZED VIEW shtb_full_view2;
DROP MATERIALIZED VIEW shtb_aggr_view1;
DROP MATERIALIZED VIEW shtb_aggr_view2;
DROP MATERIALIZED VIEW shtb_view_on_native;

DROP TABLE shtb_test1;
DROP TABLE shtb_test2;
DROP DATABASE test1;

USE test2;
DROP MATERIALIZED VIEW shtb_test1_view1;
DROP MATERIALIZED VIEW shtb_test2_view2;
DROP TABLE shtb_test1;
DROP TABLE shtb_test2;
DROP DATABASE test2;
