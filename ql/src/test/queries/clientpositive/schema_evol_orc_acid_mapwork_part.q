set hive.cli.print.header=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;
SET hive.exec.schema.evolution=false;
SET hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=none;
set hive.exec.dynamic.partition.mode=nonstrict;


-- SORT_QUERY_RESULTS
--
-- FILE VARIATION: ORC, ACID Non-Vectorized, MapWork, Partitioned
-- *IMPORTANT NOTE* We set hive.exec.schema.evolution=false above since schema evolution is always used for ACID.
--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... STATIC INSERT
---
CREATE TABLE partitioned1(a INT, b STRING) PARTITIONED BY(part INT) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table partitioned1 partition(part=1) values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned1 add columns(c int, d string);

insert into table partitioned1 partition(part=2) values(1, 'new', 10, 'ten'),(2, 'new', 20, 'twenty'), (3, 'new', 30, 'thirty'),(4, 'new', 40, 'forty');

insert into table partitioned1 partition(part=1) values(5, 'new', 100, 'hundred'),(6, 'new', 200, 'two hundred');

-- SELECT permutation columns to make sure NULL defaulting works right
select part,a,b from partitioned1;
select part,a,b,c from partitioned1;
select part,a,b,c,d from partitioned1;
select part,a,c,d from partitioned1;
select part,a,d from partitioned1;
select part,c from partitioned1;
select part,d from partitioned1;

--
-- SECTION VARIATION: ALTER TABLE CHANGE COLUMN ... STATIC INSERT
-- smallint = (2-byte signed integer, from -32,768 to 32,767)
--
CREATE TABLE partitioned2(a smallint, b STRING) PARTITIONED BY(part INT) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table partitioned2 partition(part=1) values(1000, 'original'),(6737, 'original'), ('3', 'original'),('4', 'original');

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table partitioned2 change column a a int;

insert into table partitioned2 partition(part=2) values(72909, 'new'),(200, 'new'), (32768, 'new'),(40000, 'new');

insert into table partitioned2 partition(part=1) values(5000, 'new'),(90000, 'new');

select part,a,b from partitioned2;


--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... DYNAMIC INSERT
---
CREATE TABLE partitioned3(a INT, b STRING) PARTITIONED BY(part INT) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table partitioned3 partition(part=1) values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned3 add columns(c int, d string);

insert into table partitioned3 partition(part) values(1, 'new', 10, 'ten', 2),(2, 'new', 20, 'twenty', 2), (3, 'new', 30, 'thirty', 2),(4, 'new', 40, 'forty', 2),
    (5, 'new', 100, 'hundred', 1),(6, 'new', 200, 'two hundred', 1);

-- SELECT permutation columns to make sure NULL defaulting works right
select part,a,b from partitioned1;
select part,a,b,c from partitioned1;
select part,a,b,c,d from partitioned1;
select part,a,c,d from partitioned1;
select part,a,d from partitioned1;
select part,c from partitioned1;
select part,d from partitioned1;


--
-- SECTION VARIATION: ALTER TABLE CHANGE COLUMN ... DYNAMIC INSERT
-- smallint = (2-byte signed integer, from -32,768 to 32,767)
--
CREATE TABLE partitioned4(a smallint, b STRING) PARTITIONED BY(part INT) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table partitioned4 partition(part=1) values(1000, 'original'),(6737, 'original'), ('3', 'original'),('4', 'original');

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table partitioned4 change column a a int;

insert into table partitioned4 partition(part) values(72909, 'new', 2),(200, 'new', 2), (32768, 'new', 2),(40000, 'new', 2),
    (5000, 'new', 1),(90000, 'new', 1);

select part,a,b from partitioned4;


--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... UPDATE New Columns
---
CREATE TABLE partitioned5(a INT, b STRING) PARTITIONED BY(part INT) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table partitioned5 partition(part=1) values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned5 add columns(c int, d string);

insert into table partitioned5 partition(part=2) values(1, 'new', 10, 'ten'),(2, 'new', 20, 'twenty'), (3, 'new', 30, 'thirty'),(4, 'new', 40, 'forty');

insert into table partitioned5 partition(part=1) values(5, 'new', 100, 'hundred'),(6, 'new', 200, 'two hundred');

select part,a,b,c,d from partitioned5;

-- UPDATE New Columns
update partitioned5 set c=99;

select part,a,b,c,d from partitioned5;


--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... DELETE where old column
---
CREATE TABLE partitioned6(a INT, b STRING) PARTITIONED BY(part INT) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table partitioned6 partition(part=1) values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned6 add columns(c int, d string);

insert into table partitioned6 partition(part=2) values(1, 'new', 10, 'ten'),(2, 'new', 20, 'twenty'), (3, 'new', 30, 'thirty'),(4, 'new', 40, 'forty');

insert into table partitioned6 partition(part=1) values(5, 'new', 100, 'hundred'),(6, 'new', 200, 'two hundred');

select part,a,b,c,d from partitioned6;

-- DELETE where old column
delete from partitioned6 where a = 2 or a = 4 or a = 6;

select part,a,b,c,d from partitioned6;


--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... DELETE where new column
---
CREATE TABLE partitioned7(a INT, b STRING) PARTITIONED BY(part INT) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table partitioned7 partition(part=1) values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table partitioned7 add columns(c int, d string);

insert into table partitioned7 partition(part=2) values(1, 'new', 10, 'ten'),(2, 'new', 20, 'twenty'), (3, 'new', 30, 'thirty'),(4, 'new', 40, 'forty');

insert into table partitioned7 partition(part=1) values(5, 'new', 100, 'hundred'),(6, 'new', 200, 'two hundred');

select part,a,b,c,d from partitioned7;

-- DELETE where new column
delete from partitioned7 where a = 1 or c = 30 or c == 100;

select part,a,b,c,d from partitioned7;


DROP TABLE partitioned1;
DROP TABLE partitioned2;
DROP TABLE partitioned3;
DROP TABLE partitioned4;
DROP TABLE partitioned5;
DROP TABLE partitioned6;
DROP TABLE partitioned7;