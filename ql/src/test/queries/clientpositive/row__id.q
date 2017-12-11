-- tid is flaky when compute column stats
set hive.stats.column.autogather=false;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists hello_acid;
create table hello_acid (key int, value int)
partitioned by (load_date date)
clustered by(key) into 3 buckets
stored as orc tblproperties ('transactional'='true');

insert into hello_acid partition (load_date='2016-03-01') values (1, 1);
insert into hello_acid partition (load_date='2016-03-02') values (2, 2);
insert into hello_acid partition (load_date='2016-03-03') values (3, 3);

explain
select tid from (select row__id.transactionid as tid from hello_acid) sub order by tid;

select tid from (select row__id.transactionid as tid from hello_acid) sub order by tid;

explain
select tid from (select row__id.transactionid as tid from hello_acid) sub where tid = 3;

select tid from (select row__id.transactionid as tid from hello_acid) sub where tid = 3;

