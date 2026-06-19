set hive.vectorized.execution.enabled=true;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists newtable;
create table newtable(
            a string,
            b int,
            c double)
row format delimited
fields terminated by '\t'
stored as textfile;

drop table if exists newtable_acid;
create table newtable_acid (b int, a varchar(50),c decimal(3,2), d int)
clustered by (b) into 2 buckets
stored as orc
tblproperties ('transactional'='true');

insert into newtable_acid(a,b,c)
select * from newtable;
