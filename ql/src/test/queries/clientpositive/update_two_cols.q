set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create table acid_utc(a int, b varchar(128), c float) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_utc select cint, cast(cstring1 as varchar(128)), cfloat from alltypesorc where cint < 0 order by cint limit 10;

select * from acid_utc order by a;

update acid_utc set b = 'fred',c = 3.14;

select * from acid_utc order by a;


