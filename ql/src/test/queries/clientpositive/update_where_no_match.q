set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

create table acid_wnm(a int, b varchar(128)) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_wnm select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null order by cint limit 10;

select a,b from acid_wnm order by a;

update acid_wnm set b = 'fred' where b = 'nosuchvalue';

select * from acid_wnm order by a;


