set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists acid1;
drop table if exists acid2;

create table acid1 (key int, value string) clustered by (key) into 2 buckets stored as orc tblproperties ("transactional"="true");
create table acid2 (key int, value string) clustered by (key) into 2 buckets stored as orc tblproperties ("transactional"="true");

insert into acid1 values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f'), (7, 'g'), (8, 'h');
insert into acid2 values (1,'a'),(3,'c'),(5,'e'),(7,'g');
alter table acid2 update statistics set('numRows'='210', 'rawDataSize'='840');
alter table acid1 update statistics set('numRows'='316', 'rawDataSize'='1265');
explain
select count(*) from acid1 join acid2 on acid1.key = acid2.key;
select count(*) from acid1 join acid2 on acid1.key = acid2.key;

drop table acid1;
drop table acid2;
