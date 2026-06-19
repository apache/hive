set hive.stats.dbclass=fs;
set hive.stats.fetch.column.stats=true;
set datanucleus.cache.collections=false;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

set hive.stats.autogather=true;
set hive.stats.column.autogather=true;
set hive.compute.query.using.stats=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;

set hive.fetch.task.conversion=none;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.query.results.cache.enabled=false;

-- test various alter commands

create table stats_nonpart(key int,value string) tblproperties ("transactional"="true", "transactional_properties"="insert_only");
insert into table stats_nonpart values (1, "foo");
explain select count(key) from stats_nonpart;

ALTER TABLE stats_nonpart CHANGE COLUMN key key2 int;
explain select count(key2) from stats_nonpart;
explain select count(value) from stats_nonpart;
analyze table stats_nonpart compute statistics for columns;
explain select count(key2) from stats_nonpart;

alter table stats_nonpart rename to stats_nonpart2;
explain select count(key2) from stats_nonpart2;
analyze table stats_nonpart2 compute statistics for columns;
explain select count(key2) from stats_nonpart2;

alter table stats_nonpart2 set serde 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
alter table stats_nonpart2 set serdeproperties ("foo"="bar");
alter table stats_nonpart2 set fileformat rcfile;
explain select count(key2) from stats_nonpart2;

alter table stats_nonpart2 set location 'file:${system:test.tmp.dir}/stats_nonpart_zzz';
explain select count(key2) from stats_nonpart2;

insert overwrite table stats_nonpart2 values (1, "foo");
explain select count(key2) from stats_nonpart2;

alter table stats_nonpart2 add constraint primary_key primary key (key2) disable novalidate rely;
alter table stats_nonpart2 drop constraint primary_key;
explain select count(key2) from stats_nonpart2;

alter table stats_nonpart2 clustered by (key2) INTO 2 BUCKETS;
explain select count(key2) from stats_nonpart2;
insert into table stats_nonpart2 values (2, "foo");
explain select count(key2) from stats_nonpart2;


drop table stats_nonpart2;


create table stats_part(key int,value string) partitioned by (p int) tblproperties ("transactional"="true", "transactional_properties"="insert_only");
insert into table stats_part partition(p=101) values (1, "foo");
insert into table stats_part partition(p=102) values (2, "bar");
insert into table stats_part partition(p=103) values (3, "baz");

alter table stats_part partition column (p decimal(10,0));
explain select count(key) from stats_part;

analyze table stats_part partition(p) compute statistics for columns;
explain select count(key) from stats_part;

alter table stats_part partition(p=102) rename to partition (p=104);
explain select count(key) from stats_part where p = 101;
explain select count(key) from stats_part;

analyze table stats_part partition(p) compute statistics for columns;
explain select count(key) from stats_part;

ALTER TABLE stats_part CHANGE COLUMN key key2 int;
explain select count(key2) from stats_part;
explain select count(value) from stats_part;

analyze table stats_part partition(p) compute statistics for columns;
explain select count(key2) from stats_part;

alter table stats_part add partition(p=105);
explain select count(key2) from stats_part;
analyze table stats_part partition(p) compute statistics for columns;
explain select count(key2) from stats_part;

alter table stats_part drop partition(p=104);
explain select count(key2) from stats_part;



drop table stats_part;



