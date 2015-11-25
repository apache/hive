set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


create table acid_notbucketed(a int, b varchar(128)) stored as orc;

insert into table acid_notbucketed values (1, 'abc'), (2, 'def');

select * from acid_notbucketed;
