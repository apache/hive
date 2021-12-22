set hive.cbo.enable = True;
set hive.vectorized.execution.enabled = True;

create table alter1(a int, b int);
explain ddl select * from alter1;
alter table alter1 set tblproperties ('a'='1', 'c'='3');
explain ddl select * from alter1;
alter table alter1 set tblproperties ('a'='1', 'c'='4', 'd'='3');
explain ddl select * from alter1;

alter table alter1 set tblproperties ('EXTERNAL'='TRUE');
explain ddl select * from alter1;
alter table alter1 set tblproperties ('EXTERNAL'='FALSE');
explain ddl select * from alter1;

alter table alter1 set serdeproperties('s1'='9');
explain ddl select * from alter1;
alter table alter1 set serdeproperties('s1'='10', 's2' ='20');
explain ddl select * from alter1;
drop table alter1;

CREATE DATABASE alter1_db;
SHOW TABLES alter1_db;

CREATE TABLE alter1_db.alter1(a INT, b INT);
explain ddl select * from alter1_db.alter1;

ALTER TABLE alter1_db.alter1 SET TBLPROPERTIES ('a'='1', 'c'='3');
explain ddl select * from alter1_db.alter1;

ALTER TABLE alter1_db.alter1 SET TBLPROPERTIES ('a'='1', 'c'='4', 'd'='3');
explain ddl select * from alter1_db.alter1;

ALTER TABLE alter1_db.alter1 SET TBLPROPERTIES ('EXTERNAL'='TRUE');
explain ddl select * from alter1_db.alter1;

ALTER TABLE alter1_db.alter1 SET TBLPROPERTIES ('EXTERNAL'='FALSE');
explain ddl select * from alter1_db.alter1;

ALTER TABLE alter1_db.alter1 SET SERDEPROPERTIES('s1'='9');
explain ddl select * from alter1_db.alter1;

ALTER TABLE alter1_db.alter1 SET SERDEPROPERTIES('s1'='10', 's2' ='20');
explain ddl select * from alter1_db.alter1;

DROP TABLE alter1_db.alter1;
DROP DATABASE alter1_db;

create table alter2(a int, b int) partitioned by (insertdate string);
explain ddl select * from alter2;
show partitions alter2;
alter table alter2 add partition (insertdate='2008-01-01') location '2008/01/01';
explain ddl select * from alter2;
show partitions alter2;
alter table alter2 add partition (insertdate='2008-01-02') location '2008/01/02';
explain ddl select * from alter2;
show partitions alter2;
drop table alter2;

create external table alter2(a int, b int) partitioned by (insertdate string);
explain ddl select * from alter2;
show partitions alter2;
alter table alter2 add partition (insertdate='2008-01-01') location '2008/01/01';
explain ddl select * from alter2;
show partitions alter2;
alter table alter2 add partition (insertdate='2008-01-02') location '2008/01/02';
explain ddl select * from alter2;
show partitions alter2;

-- Cleanup
DROP TABLE alter2;
SHOW TABLES LIKE "alter*";

-- Using non-default Database

CREATE DATABASE alter2_db;
USE alter2_db;
SHOW TABLES LIKE "alter*";

CREATE TABLE alter2(a int, b int) PARTITIONED BY (insertdate string);
explain ddl select * from alter2;
SHOW PARTITIONS alter2;
ALTER TABLE alter2 ADD PARTITION (insertdate='2008-01-01') LOCATION '2008/01/01';
explain ddl select * from alter2;
SHOW PARTITIONS alter2;
ALTER TABLE alter2 ADD PARTITION (insertdate='2008-01-02') LOCATION '2008/01/02';
explain ddl select * from alter2;
SHOW PARTITIONS alter2;
DROP TABLE alter2;

CREATE EXTERNAL TABLE alter2(a int, b int) PARTITIONED BY (insertdate string);
explain ddl select * from alter2;
SHOW PARTITIONS alter2;
ALTER TABLE alter2 ADD PARTITION (insertdate='2008-01-01') LOCATION '2008/01/01';
explain ddl select * from alter2;
SHOW PARTITIONS alter2;
ALTER TABLE alter2 ADD PARTITION (insertdate='2008-01-02') LOCATION '2008/01/02';
explain ddl select * from alter2;
SHOW PARTITIONS alter2;

DROP TABLE alter2;
USE default;
DROP DATABASE alter2_db;