set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;

set hive.optimize.sort.dynamic.partition=false;

-- single level partition, sorted dynamic partition disabled
drop table acid;
CREATE TABLE acid(key string, value string) PARTITIONED BY(ds string) CLUSTERED BY(key) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');
insert into table acid partition(ds)  select key,value,ds from srcpart;
select count(*) from acid where ds='2008-04-08';

insert into table acid partition(ds='2008-04-08') values("foo", "bar");
select count(*) from acid where ds='2008-04-08';

explain update acid set value = 'bar' where key = 'foo' and ds='2008-04-08';
update acid set value = 'bar' where key = 'foo' and ds='2008-04-08';
select count(*) from acid where ds='2008-04-08';

explain update acid set value = 'bar' where key = 'foo' and ds in ('2008-04-08');
update acid set value = 'bar' where key = 'foo' and ds in ('2008-04-08');
select count(*) from acid where ds in ('2008-04-08');

delete from acid where key = 'foo' and ds='2008-04-08';
select count(*) from acid where ds='2008-04-08';

set hive.optimize.sort.dynamic.partition=true;

-- single level partition, sorted dynamic partition enabled
drop table acid;
CREATE TABLE acid(key string, value string) PARTITIONED BY(ds string) CLUSTERED BY(key) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');
insert into table acid partition(ds)  select key,value,ds from srcpart;
select count(*) from acid where ds='2008-04-08';

insert into table acid partition(ds='2008-04-08') values("foo", "bar");
select count(*) from acid where ds='2008-04-08';

explain update acid set value = 'bar' where key = 'foo' and ds='2008-04-08';
update acid set value = 'bar' where key = 'foo' and ds='2008-04-08';
select count(*) from acid where ds='2008-04-08';

explain update acid set value = 'bar' where key = 'foo' and ds in ('2008-04-08');
update acid set value = 'bar' where key = 'foo' and ds in ('2008-04-08');
select count(*) from acid where ds in ('2008-04-08');

delete from acid where key = 'foo' and ds='2008-04-08';
select count(*) from acid where ds='2008-04-08';

set hive.optimize.sort.dynamic.partition=false;

-- 2 level partition, sorted dynamic partition disabled
drop table acid;
CREATE TABLE acid(key string, value string) PARTITIONED BY(ds string, hr int) CLUSTERED BY(key) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');
insert into table acid partition(ds,hr)  select * from srcpart;
select count(*) from acid where ds='2008-04-08' and hr=11;

insert into table acid partition(ds='2008-04-08',hr=11) values("foo", "bar");
select count(*) from acid where ds='2008-04-08' and hr=11;

explain update acid set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr=11;
update acid set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr=11;
select count(*) from acid where ds='2008-04-08' and hr=11;

explain update acid set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr>=11;
update acid set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr>=11;
select count(*) from acid where ds='2008-04-08' and hr>=11;

delete from acid where key = 'foo' and ds='2008-04-08' and hr=11;
select count(*) from acid where ds='2008-04-08' and hr=11;

set hive.optimize.sort.dynamic.partition=true;

-- 2 level partition, sorted dynamic partition enabled
drop table acid;
CREATE TABLE acid(key string, value string) PARTITIONED BY(ds string, hr int) CLUSTERED BY(key) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');
insert into table acid partition(ds,hr)  select * from srcpart;
select count(*) from acid where ds='2008-04-08' and hr=11;

insert into table acid partition(ds='2008-04-08',hr=11) values("foo", "bar");
select count(*) from acid where ds='2008-04-08' and hr=11;

explain update acid set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr=11;
update acid set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr=11;
select count(*) from acid where ds='2008-04-08' and hr=11;

explain update acid set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr>=11;
update acid set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr>=11;
select count(*) from acid where ds='2008-04-08' and hr>=11;

delete from acid where key = 'foo' and ds='2008-04-08' and hr=11;
select count(*) from acid where ds='2008-04-08' and hr=11;

set hive.optimize.sort.dynamic.partition=true;
set hive.optimize.constant.propagation=false;

-- 2 level partition, sorted dynamic partition enabled, constant propagation disabled
drop table acid;
CREATE TABLE acid(key string, value string) PARTITIONED BY(ds string, hr int) CLUSTERED BY(key) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true');
insert into table acid partition(ds,hr)  select * from srcpart;
select count(*) from acid where ds='2008-04-08' and hr=11;

insert into table acid partition(ds='2008-04-08',hr=11) values("foo", "bar");
select count(*) from acid where ds='2008-04-08' and hr=11;

explain update acid set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr=11;
update acid set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr=11;
select count(*) from acid where ds='2008-04-08' and hr=11;

explain update acid set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr>=11;
update acid set value = 'bar' where key = 'foo' and ds='2008-04-08' and hr>=11;
select count(*) from acid where ds='2008-04-08' and hr>=11;

delete from acid where key = 'foo' and ds='2008-04-08' and hr=11;
select count(*) from acid where ds='2008-04-08' and hr=11;

set hive.optimize.sort.dynamic.partition=true;
