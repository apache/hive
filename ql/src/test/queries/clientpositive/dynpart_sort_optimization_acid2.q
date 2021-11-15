--! qt:dataset:srcpart
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.optimize.clustered.sort=false;
set hive.optimize.sort.dynamic.partition.threshold=1;

CREATE TABLE non_acid(key string, value string)
PARTITIONED BY(ds string, hr int)
CLUSTERED BY(key) INTO 2 BUCKETS
STORED AS ORC;

explain
insert into table non_acid partition(ds,hr) select * from srcpart sort by value;
