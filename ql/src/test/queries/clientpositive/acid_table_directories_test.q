--! qt:disabled:disabled Tests the output of LS -R and that changes, Post Hadoop 3.3.x the output isn't sorted, so
--disabled as part of HIVE-24484 (Upgrade Hadoop to 3.3.1)
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.qtest.additional.partial.mask.pattern=.*acidparttable\/p=(100|200)\/base.*,.*acidparttable/p=(100|200)/delta.*;
set hive.qtest.additional.partial.mask.replacement.text=ACID BASE DIR,ACID DELTA DIR;

-- create a source table where the IOW data select from
create table srctbl (key char(1), value int);
insert into table srctbl values ('d', 4), ('e', 5), ('f', 6), ('i', 9), ('j', 10);
select * from srctbl;

-- insert overwrite on partitioned acid table
drop table if exists acidparttbl;
create table acidparttbl (key char(1), value int) partitioned by (p int) clustered by (value) into 2 buckets  stored as orc   location 'pfile://${system:test.tmp.dir}/acidparttable' TBLPROPERTIES ("transactional"="true");

insert into table acidparttbl partition(p=100) values ('a', 1), ('b', 2), ('c', 3);
select p, key, value from acidparttbl order by p, key;

insert overwrite table acidparttbl partition(p=100) select key, value from srctbl where key in ('d', 'e', 'f');
select p, key, value from acidparttbl order by p, key;

insert into table acidparttbl partition(p) values ('g', 7, 100), ('h', 8, 200);
select p, key, value from acidparttbl order by p, key;

insert overwrite table acidparttbl partition(p) values ('i', 9, 100), ('j', 10, 200);
select p, key, value from acidparttbl order by p, key;

-- check directories of the table
dfs -ls -R 'pfile://${system:test.tmp.dir}/acidparttable';

drop table acidparttbl;
drop table srctbl;