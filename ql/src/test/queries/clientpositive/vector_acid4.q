--! qt:dataset:src

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition=true;
set hive.vectorized.execution.enabled=true;
set hive.compute.query.using.stats=false;
set hive.fetch.task.conversion=none;
set hive.llap.io.enabled=true;
set hive.compute.query.using.stats=false;
SET hive.exec.orc.default.row.index.stride=1000;
set hive.mapred.mode=nonstrict;

set hive.exec.orc.delta.streaming.optimizations.enabled=true;


drop table cross_numbers;
create table cross_numbers(i string);
insert into table cross_numbers select key from src limit 20;

drop table lots_of_rows;
create table lots_of_rows(key string) stored as orc tblproperties("transactional"="false");
insert into table lots_of_rows select concat(key, '', i) from src cross join cross_numbers;

drop table testacid1;
create table testacid1(id string, id2 string) clustered by (id2) into 2 buckets stored as orc tblproperties("transactional"="true");
insert into table testacid1 select key, key from lots_of_rows;

drop table lots_of_row;

select * from testacid1 order by id limit 30;
select sum(hash(*)) from testacid1 limit 10;

select count(id) from testacid1;

select count(1) from testacid1;

select count(1) from testacid1 where id = '0128';

explain update testacid1 set id = '206' where id = '0128';
update testacid1 set id = '206' where id = '0128';

select * from testacid1 order by id limit 30;
