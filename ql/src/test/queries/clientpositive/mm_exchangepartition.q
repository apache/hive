set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop database if exists ex1;
drop database if exists ex2;

create database ex1;
create database ex2;

CREATE TABLE ex1.exchange_part_test1 (f1 string) PARTITIONED BY (ds STRING) TBLPROPERTIES ("transactional"="true", "transactional_properties"="insert_only");
CREATE TABLE ex2.exchange_part_test2 (f1 string) PARTITIONED BY (ds STRING) TBLPROPERTIES ("transactional"="true", "transactional_properties"="insert_only");
SHOW PARTITIONS ex1.exchange_part_test1;
SHOW PARTITIONS ex2.exchange_part_test2;

ALTER TABLE ex2.exchange_part_test2 ADD PARTITION (ds='2013-04-05');
SHOW PARTITIONS ex1.exchange_part_test1;
SHOW PARTITIONS ex2.exchange_part_test2;

ALTER TABLE ex1.exchange_part_test1 EXCHANGE PARTITION (ds='2013-04-05') WITH TABLE ex2.exchange_part_test2;
SHOW PARTITIONS ex1.exchange_part_test1;
SHOW PARTITIONS ex2.exchange_part_test2;


DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t5;
DROP TABLE IF EXISTS t6;

CREATE TABLE t1 (a int) PARTITIONED BY (d1 int) TBLPROPERTIES ("transactional"="true", "transactional_properties"="insert_only");
CREATE TABLE t2 (a int) PARTITIONED BY (d1 int) TBLPROPERTIES ("transactional"="true", "transactional_properties"="insert_only");
CREATE TABLE t3 (a int) PARTITIONED BY (d1 int, d2 int) TBLPROPERTIES ("transactional"="true", "transactional_properties"="insert_only");
CREATE TABLE t4 (a int) PARTITIONED BY (d1 int, d2 int) TBLPROPERTIES ("transactional"="true", "transactional_properties"="insert_only");
CREATE TABLE t5 (a int) PARTITIONED BY (d1 int, d2 int, d3 int) TBLPROPERTIES ("transactional"="true", "transactional_properties"="insert_only");
CREATE TABLE t6 (a int) PARTITIONED BY (d1 int, d2 int, d3 int) TBLPROPERTIES ("transactional"="true", "transactional_properties"="insert_only");
set hive.mapred.mode=nonstrict;

INSERT INTO TABLE t1 PARTITION (d1 = 1) SELECT key FROM src where key = 100 limit 1;
INSERT INTO TABLE t3 PARTITION (d1 = 1, d2 = 1) SELECT key FROM src where key = 100 limit 1;
INSERT INTO TABLE t5 PARTITION (d1 = 1, d2 = 1, d3=1) SELECT key FROM src where key = 100 limit 1;

SELECT * FROM t1;

SELECT * FROM t3;

SELECT * FROM t5;

ALTER TABLE t2 EXCHANGE PARTITION (d1 = 1) WITH TABLE t1;
SELECT * FROM t1;
SELECT * FROM t2;

ALTER TABLE t4 EXCHANGE PARTITION (d1 = 1, d2 = 1) WITH TABLE t3;
SELECT * FROM t3;
SELECT * FROM t4;

ALTER TABLE t6 EXCHANGE PARTITION (d1 = 1, d2 = 1, d3 = 1) WITH TABLE t5;
SELECT * FROM t5;
SELECT * FROM t6;

DROP DATABASE ex1 CASCADE;
DROP DATABASE ex2 CASCADE;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
DROP TABLE t6;
