set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.stats.fetch.column.stats=true;
set hive.etl.execution.engine=tez;

create table emps_hive (
  empid int,
  deptno int,
  name varchar(256),
  salary float,
  commission int)
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

create table depts_hive (
  deptno int,
  name varchar(256),
  locationid int)
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

alter table emps_hive add constraint pk1 primary key (empid) disable novalidate rely;
alter table depts_hive add constraint pk2 primary key (deptno) disable novalidate rely;

alter table emps_hive add constraint fk1 foreign key (deptno) references depts_hive(deptno) disable novalidate rely;

explain
select empid, depts_hive.deptno from emps_hive
join depts_hive using (deptno) where depts_hive.deptno > cast(ltrim('10') as integer)
group by empid, depts_hive.deptno;

explain
create table tab1_hive
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only') as
select empid, depts_hive.deptno from emps_hive
join depts_hive using (deptno) where depts_hive.deptno > cast(ltrim('10') as integer)
group by empid, depts_hive.deptno;

create table tab1_hive (
  empid int,
  deptno int
)
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

explain
insert into tab1_hive
select empid, deptno
from emps_hive;

-- test to verify engine switches back to impala after tez execution
explain
select empid, depts_hive.deptno from tab1_hive
join depts_hive using (deptno) where depts_hive.deptno > cast(ltrim('10') as integer)
group by empid, depts_hive.deptno;

explain
insert into tab1_hive
select empid, deptno
from emps_hive
limit 10;

create table tab1_hive_part (
  empid int,
  deptno int
)
PARTITIONED BY (partcol string)
stored as parquet TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only');

explain
insert into tab1_hive_part
select empid, deptno, name
from emps_hive;

explain
insert into tab1_hive_part
select empid, deptno, name
from emps_hive
limit 10;

CREATE TABLE test_dest(i int, s string) STORED AS ORC TBLPROPERTIES ('transactional'='true');
CREATE TABLE test_updates(i int, s string) STORED AS ORC TBLPROPERTIES ('transactional'='true');
-- should be executed via tez
EXPLAIN MERGE INTO test_dest USING test_updates ON test_dest.i = test_updates.i
WHEN MATCHED THEN UPDATE
SET s = test_updates.s
WHEN NOT MATCHED THEN INSERT
VALUES (test_updates.i, test_updates.s);

-- test to verify engine switches back to impala after tez execution
EXPLAIN SELECT * from tab1_hive;

-- should be planned for tez
EXPLAIN CREATE MATERIALIZED VIEW test_mv_dest AS SELECT i FROM test_dest WHERE s IS NOT NULL;

-- should be planned for tez
EXPLAIN UPDATE test_dest SET i = 3 WHERE i = 4;

-- test to verify engine switches back to impala after tez execution
EXPLAIN SELECT * from tab1_hive;

-- should be planned for tez
EXPLAIN DELETE FROM test_dest WHERE i = 4;

-- test to verify engine switches back to impala after tez execution
EXPLAIN SELECT * from tab1_hive;

drop table tab1_hive;
drop table tab1_hive_part;
drop table emps_hive;
drop table depts_hive;
drop table test_dest;
drop table test_updates;
