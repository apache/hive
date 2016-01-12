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
-- FILE VARIATION: ORC, ACID Non-Vectorized, MapWork, Table
-- *IMPORTANT NOTE* We set hive.exec.schema.evolution=false above since schema evolution is always used for ACID.
--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... STATIC INSERT
---
CREATE TABLE table1(a INT, b STRING) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table table1 values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table table1 add columns(c int, d string);

insert into table table1 values(1, 'new', 10, 'ten'),(2, 'new', 20, 'twenty'), (3, 'new', 30, 'thirty'),(4, 'new', 40, 'forty');

insert into table table1 values(5, 'new', 100, 'hundred'),(6, 'new', 200, 'two hundred');

-- SELECT permutation columns to make sure NULL defaulting works right
select a,b from table1;
select a,b,c from table1;
select a,b,c,d from table1;
select a,c,d from table1;
select a,d from table1;
select c from table1;
select d from table1;

--
-- SECTION VARIATION: ALTER TABLE CHANGE COLUMN ... STATIC INSERT
-- smallint = (2-byte signed integer, from -32,768 to 32,767)
--
CREATE TABLE table2(a smallint, b STRING) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table table2 values(1000, 'original'),(6737, 'original'), ('3', 'original'),('4', 'original');

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table2 change column a a int;

insert into table table2 values(72909, 'new'),(200, 'new'), (32768, 'new'),(40000, 'new');

insert into table table2 values(5000, 'new'),(90000, 'new');

select a,b from table2;



--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... UPDATE New Columns
---
CREATE TABLE table5(a INT, b STRING) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table table5 values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table table5 add columns(c int, d string);

insert into table table5 values(1, 'new', 10, 'ten'),(2, 'new', 20, 'twenty'), (3, 'new', 30, 'thirty'),(4, 'new', 40, 'forty');

insert into table table5 values(5, 'new', 100, 'hundred'),(6, 'new', 200, 'two hundred');

select a,b,c,d from table5;

-- UPDATE New Columns
update table5 set c=99;

select a,b,c,d from table5;


--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... DELETE where old column
---
CREATE TABLE table6(a INT, b STRING) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table table6 values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table table6 add columns(c int, d string);

insert into table table6 values(1, 'new', 10, 'ten'),(2, 'new', 20, 'twenty'), (3, 'new', 30, 'thirty'),(4, 'new', 40, 'forty');

insert into table table6 values(5, 'new', 100, 'hundred'),(6, 'new', 200, 'two hundred');

select a,b,c,d from table6;

-- DELETE where old column
delete from table6 where a = 2 or a = 4 or a = 6;

select a,b,c,d from table6;


--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... DELETE where new column
---
CREATE TABLE table7(a INT, b STRING) clustered by (a) into 2 buckets STORED AS ORC TBLPROPERTIES ('transactional'='true');

insert into table table7 values(1, 'original'),(2, 'original'), (3, 'original'),(4, 'original');

-- Table-Non-Cascade ADD COLUMNS ...
alter table table7 add columns(c int, d string);

insert into table table7 values(1, 'new', 10, 'ten'),(2, 'new', 20, 'twenty'), (3, 'new', 30, 'thirty'),(4, 'new', 40, 'forty');

insert into table table7 values(5, 'new', 100, 'hundred'),(6, 'new', 200, 'two hundred');

select a,b,c,d from table7;

-- DELETE where new column
delete from table7 where a = 1 or c = 30 or c == 100;

select a,b,c,d from table7;


DROP TABLE table1;
DROP TABLE table2;
DROP TABLE table5;
DROP TABLE table6;
DROP TABLE table7;