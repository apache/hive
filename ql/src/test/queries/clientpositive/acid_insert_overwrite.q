set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition.mode=nonstrict;

-- create a source table where the IOW data select from
create table srctbl (key char(1), value int);
insert into table srctbl values ('d', 4), ('e', 5), ('f', 6), ('i', 9), ('j', 10);
select * from srctbl;

-- insert overwrite on non-partitioned acid table
drop table if exists acidtbl1;
create table acidtbl1 (key char(1), value int) clustered by (value) into 2 buckets stored as orc TBLPROPERTIES ("transactional"="true");

insert into table acidtbl1 values ('a', 1), ('b', 2), ('c', 3);
select * from acidtbl1 order by key;

insert overwrite table acidtbl1 select key, value from srctbl where key in ('d', 'e', 'f');
select * from acidtbl1 order by key;

insert into table acidtbl1 values ('g', 7), ('h', 8);
select * from acidtbl1 order by key;

insert overwrite table acidtbl1 select key, value from srctbl where key in ('i', 'j');
select * from acidtbl1 order by key;

insert into table acidtbl1 values ('k', 11);
insert into table acidtbl1 values ('l', 12);
select * from acidtbl1 order by key;


-- insert overwrite with multi table insert
drop table if exists acidtbl2;
create table acidtbl2 (key char(1), value int) clustered by (value) into 2 buckets stored as orc TBLPROPERTIES ("transactional"="true");

drop table if exists acidtbl3;
create table acidtbl3 (key char(1), value int) clustered by (value) into 2 buckets stored as orc TBLPROPERTIES ("transactional"="true");

insert into table acidtbl2 values ('m', 13), ('n', 14);
select * from acidtbl2 order by key;

insert into table acidtbl3 values ('o', 15), ('p', 16);
select * from acidtbl3 order by key;

from acidtbl1
insert overwrite table acidtbl2 select key, value
insert into table acidtbl3 select key, value;

select * from acidtbl2 order by key;
select * from acidtbl3 order by key;

drop table acidtbl1;
drop table acidtbl2;
drop table acidtbl3;


-- insert overwrite on partitioned acid table
drop table if exists acidparttbl;
create table acidparttbl (key char(1), value int) partitioned by (p int) clustered by (value) into 2 buckets stored as orc TBLPROPERTIES ("transactional"="true");

insert into table acidparttbl partition(p=100) values ('a', 1), ('b', 2), ('c', 3);
select p, key, value from acidparttbl order by p, key;

insert overwrite table acidparttbl partition(p=100) select key, value from srctbl where key in ('d', 'e', 'f');
select p, key, value from acidparttbl order by p, key;

insert into table acidparttbl partition(p) values ('g', 7, 100), ('h', 8, 200);
select p, key, value from acidparttbl order by p, key;

insert overwrite table acidparttbl partition(p) values ('i', 9, 100), ('j', 10, 200);
select p, key, value from acidparttbl order by p, key;

drop table acidparttbl;
