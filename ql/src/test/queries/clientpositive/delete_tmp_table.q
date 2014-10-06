set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create temporary table acid_dtt(a int, b varchar(128)) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_dtt select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null order by cint limit 10;

select * from acid_dtt order by a;

delete from acid_dtt where b = '0ruyd6Y50JpdGRf6HqD' or b = '2uLyD28144vklju213J1mr';

select a,b from acid_dtt order by b;


