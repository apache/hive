set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create table acid_utt(a int, b varchar(128)) clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

insert into table acid_utt select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null order by cint limit 10;

select a,b from acid_utt order by a;

update acid_utt set a = 'fred' where b = '0ruyd6Y50JpdGRf6HqD';

select * from acid_utt order by a;


