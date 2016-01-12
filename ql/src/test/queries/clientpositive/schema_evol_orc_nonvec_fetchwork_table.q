set hive.cli.print.header=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;
SET hive.exec.schema.evolution=true;
SET hive.vectorized.execution.enabled=false;
set hive.fetch.task.conversion=more;
set hive.exec.dynamic.partition.mode=nonstrict;


-- SORT_QUERY_RESULTS
--
-- FILE VARIATION: ORC, Non-Vectorized, FetchWork, Table
--
--
-- SECTION VARIATION: ALTER TABLE ADD COLUMNS ... STATIC INSERT
---
CREATE TABLE table1(a INT, b STRING) STORED AS ORC;

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
CREATE TABLE table2(a smallint, b STRING) STORED AS ORC;

insert into table table2 values(1000, 'original'),(6737, 'original'), ('3', 'original'),('4', 'original');

-- Table-Non-Cascade CHANGE COLUMNS ...
alter table table2 change column a a int;

insert into table table2 values(72909, 'new'),(200, 'new'), (32768, 'new'),(40000, 'new');

insert into table table2 values(5000, 'new'),(90000, 'new');

select a,b from table2;


DROP TABLE table1;
DROP TABLE table2;