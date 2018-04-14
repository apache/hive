--! qt:dataset:alltypesorc
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;


create table not_an_acid_table(a int, b varchar(128));

insert into table not_an_acid_table select cint, cast(cstring1 as varchar(128)) from alltypesorc where cint is not null order by cint limit 10;

select a,b from not_an_acid_table order by a;

update not_an_acid_table set b = 'fred' where b = '0ruyd6Y50JpdGRf6HqD';
