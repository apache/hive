set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

-- This test checks that a join with tables with two different buckets send the right bucket info to each table.
create table acidjoin1(name varchar(50), age int) clustered by (age) into 2 buckets stored as orc TBLPROPERTIES ("transactional"="true"); 
create table acidjoin2(name varchar(50), gpa decimal(3, 2)) clustered by (gpa) into 4 buckets stored as orc TBLPROPERTIES ("transactional"="true"); 
create table acidjoin3(name varchar(50), age int, gpa decimal(3, 2)) clustered by (gpa) into 8 buckets stored as orc TBLPROPERTIES ("transactional"="true"); 

insert into table acidjoin1 values ('aaa', 35), ('bbb', 32), ('ccc', 32), ('ddd', 35), ('eee', 32); 
insert into table acidjoin2 values ('aaa', 3.00), ('bbb', 3.01), ('ccc', 3.02), ('ddd', 3.03), ('eee', 3.04); 

insert into table acidjoin3 select a.name, age, gpa from acidjoin1 a join acidjoin2 b on (a.name = b.name);
select * from acidjoin3 order by name;

