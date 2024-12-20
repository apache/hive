-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/

set hive.explain.user=false;

create table source(a int) stored by iceberg tblproperties ('format-version'='2') ;

insert into source values (1), (2);

create table target like source stored by iceberg;

show create table target;

select count(*) from target;

insert into target values (1), (2);

select a from target order by a;

delete from target where a=2;

select a from target order by a;

--create a partitioned iceberg table
create table emp_iceberg (id int) partitioned by (company string) stored by iceberg;

show create table emp_iceberg;

-- CTLT with the source as the partitioned iceberg table
create table emp_like1 like emp_iceberg stored by iceberg;

-- Partition column should be there
show create table emp_like1;

--create a partitioned non iceberg table
create table emp (id int) partitioned by (company string);

show create table emp;

-- CTLT with the source as the partitioned iceberg table
create table emp_like2 like emp stored by iceberg;

-- Partition column should be there
show create table emp_like2;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

-- create a managed table
create managed table man_table (id int) Stored as orc TBLPROPERTIES ('transactional'='true');

create table like_man_table like man_table stored by iceberg;

-- insert some data into the table and see if things work
insert into like_man_table values (1), (2), (3), (4);

select * from like_man_table order by id;