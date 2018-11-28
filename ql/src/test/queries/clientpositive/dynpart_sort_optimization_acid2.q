set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.optimize.sort.dynamic.partition=true;

CREATE TABLE non_acid(key string, value string)
PARTITIONED BY(ds string, hr int)
CLUSTERED BY(key) INTO 2 BUCKETS
STORED AS ORC;

explain
insert into table non_acid partition(ds,hr) select * from srcpart sort by value;
