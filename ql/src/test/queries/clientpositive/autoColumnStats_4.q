--! qt:dataset:alltypesorc
set hive.stats.column.autogather=true;
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table acid_dtt(a int, b varchar(128)) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

desc formatted acid_dtt;

explain insert into table acid_dtt select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null order by cint limit 10;

insert into table acid_dtt select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null order by cint limit 10;

desc formatted acid_dtt;

delete from acid_dtt where b = '0ruyd6Y50JpdGRf6HqD' or b = '2uLyD28144vklju213J1mr';

desc formatted acid_dtt;


