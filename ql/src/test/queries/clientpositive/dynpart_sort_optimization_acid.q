set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

set hive.exec.dynamic.partition.mode=nonstrict;

set hive.optimize.sort.dynamic.partition=false;

-- single level partition, sorted dynamic partition disabled
drop table if exists acid_part;
CREATE TABLE acid_part(key string, value string) PARTITIONED BY(ds string) CLUSTERED BY(key) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');
insert into table acid_part partition(ds)  select key,value,ds from srcpart;
-- explicitly set statistics to avoid flakiness
alter table acid_part partition(ds='2008-04-08') update statistics set('numRows'='1600', 'rawDataSize'='18000');
select count(*) from acid_part where ds='2008-04-08';

insert into table acid_part partition(ds='2008-04-08') values("foo", "bar");
select count(*) from acid_part where ds='2008-04-08';

explain update acid_part set value = 'bar' where key = 'foo' and ds='2008-04-08';
update acid_part set value = 'bar' where key = 'foo' and ds='2008-04-08';
select count(*) from acid_part where ds='2008-04-08';

explain update acid_part set value = 'bar' where key = 'foo' and ds in ('2008-04-08');
update acid_part set value = 'bar' where key = 'foo' and ds in ('2008-04-08');
select count(*) from acid_part where ds in ('2008-04-08');

delete from acid_part where key = 'foo' and ds='2008-04-08';
select count(*) from acid_part where ds='2008-04-08';

set hive.optimize.sort.dynamic.partition=true;

-- single level partition, sorted dynamic partition enabled
drop table if exists acid_part_sdpo;
CREATE TABLE acid_part_sdpo(key string, value string) PARTITIONED BY(ds string) CLUSTERED BY(key) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');
insert into table acid_part_sdpo partition(ds)  select key,value,ds from srcpart;
alter table acid_part_sdpo partition(ds='2008-04-08') update statistics set('numRows'='1600', 'rawDataSize'='18000');
select count(*) from acid_part_sdpo where ds='2008-04-08';

insert into table acid_part_sdpo partition(ds='2008-04-08') values("foo", "bar");
select count(*) from acid_part_sdpo where ds='2008-04-08';

explain update acid_part_sdpo set value = 'bar' where key = 'foo' and ds='2008-04-08';
update acid_part_sdpo set value = 'bar' where key = 'foo' and ds='2008-04-08';
select count(*) from acid_part_sdpo where ds='2008-04-08';

explain update acid_part_sdpo set value = 'bar' where key = 'foo' and ds in ('2008-04-08');
update acid_part_sdpo set value = 'bar' where key = 'foo' and ds in ('2008-04-08');
select count(*) from acid_part_sdpo where ds in ('2008-04-08');

delete from acid_part_sdpo where key = 'foo' and ds='2008-04-08';
select count(*) from acid_part_sdpo where ds='2008-04-08';

set hive.optimize.sort.dynamic.partition=false;

-- 2 level partition, sorted dynamic partition disabled
drop table if exists acid_2L_part;
CREATE TABLE acid_2L_part(key string, value string) PARTITIONED BY(ds string, hr int) CLUSTERED BY(key) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');
insert into table acid_2L_part partition(ds,hr)  select * from srcpart;
alter table acid_2L_part partition(ds='2008-04-08') update statistics set('numRows'='1600', 'rawDataSize'='18000');
select count(*) from acid_2L_part where ds='2008-04-08' and hr=11;

insert into table acid_2L_part partition(ds='2008-04-08',hr=11) values("foo", "bar");
select count(*) from acid_2L_part where ds='2008-04-08' and hr=11;

explain update acid_2L_part set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr=11;
update acid_2L_part set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr=11;
select count(*) from acid_2L_part where ds='2008-04-08' and hr=11;

explain update acid_2L_part set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr>=11;
update acid_2L_part set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr>=11;
select count(*) from acid_2L_part where ds='2008-04-08' and hr>=11;

delete from acid_2L_part where key = 'foo' and ds='2008-04-08' and hr=11;
select count(*) from acid_2L_part where ds='2008-04-08' and hr=11;

-- test with bucketing column not in select list
explain
delete from acid_2L_part where value = 'bar';
delete from acid_2L_part where value = 'bar';
select count(*) from acid_2L_part;

set hive.optimize.sort.dynamic.partition=true;

-- 2 level partition, sorted dynamic partition enabled
drop table if exists acid_2L_part_sdpo;
CREATE TABLE acid_2L_part_sdpo(key string, value string) PARTITIONED BY(ds string, hr int) CLUSTERED BY(key) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');
insert into table acid_2L_part_sdpo partition(ds,hr)  select * from srcpart;
alter table acid_2L_part_sdpo partition(ds='2008-04-08') update statistics set('numRows'='1600', 'rawDataSize'='18000');
select count(*) from acid_2L_part_sdpo where ds='2008-04-08' and hr=11;

insert into table acid_2L_part_sdpo partition(ds='2008-04-08',hr=11) values("foo", "bar");
select count(*) from acid_2L_part_sdpo where ds='2008-04-08' and hr=11;

explain update acid_2L_part_sdpo set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr=11;
update acid_2L_part_sdpo set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr=11;
select count(*) from acid_2L_part_sdpo where ds='2008-04-08' and hr=11;

explain update acid_2L_part_sdpo set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr>=11;
update acid_2L_part_sdpo set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr>=11;
select count(*) from acid_2L_part_sdpo where ds='2008-04-08' and hr>=11;

delete from acid_2L_part_sdpo where key = 'foo' and ds='2008-04-08' and hr=11;
select count(*) from acid_2L_part_sdpo where ds='2008-04-08' and hr=11;

-- test with bucketing column not in select list
explain
delete from acid_2L_part_sdpo where value = 'bar';
delete from acid_2L_part_sdpo where value = 'bar';
select count(*) from acid_2L_part_sdpo;


set hive.optimize.sort.dynamic.partition=true;
set hive.optimize.constant.propagation=false;

-- 2 level partition, sorted dynamic partition enabled, constant propagation disabled
drop table if exists acid_2L_part_sdpo_no_cp;
CREATE TABLE acid_2L_part_sdpo_no_cp(key string, value string) PARTITIONED BY(ds string, hr int) CLUSTERED BY(key) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');
insert into table acid_2L_part_sdpo_no_cp partition(ds,hr)  select * from srcpart;
alter table acid_2L_part_sdpo_no_cp partition(ds='2008-04-08') update statistics set('numRows'='1600', 'rawDataSize'='18000');
select count(*) from acid_2L_part_sdpo_no_cp where ds='2008-04-08' and hr=11;

insert into table acid_2L_part_sdpo_no_cp partition(ds='2008-04-08',hr=11) values("foo", "bar");
select count(*) from acid_2L_part_sdpo_no_cp where ds='2008-04-08' and hr=11;

explain update acid_2L_part_sdpo_no_cp set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr=11;
update acid_2L_part_sdpo_no_cp set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr=11;
select count(*) from acid_2L_part_sdpo_no_cp where ds='2008-04-08' and hr=11;

explain update acid_2L_part_sdpo_no_cp set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr>=11;
update acid_2L_part_sdpo_no_cp set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr>=11;
select count(*) from acid_2L_part_sdpo_no_cp where ds='2008-04-08' and hr>=11;

delete from acid_2L_part_sdpo_no_cp where key = 'foo' and ds='2008-04-08' and hr=11;
select count(*) from acid_2L_part_sdpo_no_cp where ds='2008-04-08' and hr=11;

set hive.optimize.sort.dynamic.partition=true;
