set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

-- This test checks that selecting from an acid table and inserting into a non-acid table works.
create table sample_06(name varchar(50), age int, gpa decimal(3, 2)) clustered by (age) into 2 buckets stored as orc TBLPROPERTIES ("transactional"="true"); 
insert into table sample_06 values ('aaa', 35, 3.00), ('bbb', 32, 3.00), ('ccc', 32, 3.00), ('ddd', 35, 3.00), ('eee', 32, 3.00); 
select * from sample_06 where gpa = 3.00;

create table tab1 (name varchar(50), age int, gpa decimal(3, 2));
insert into table tab1 select * from sample_06 where gpa = 3.00;
select * from tab1;

