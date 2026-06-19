set hive.msck.repair.batch.size=1;
set hive.mv.files.thread=0;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

DROP TABLE IF EXISTS repairtable_n6;

CREATE TABLE repairtable_n6(col STRING) PARTITIONED BY (p1 STRING, p2 STRING) STORED AS ORC tblproperties ("transactional"="true", "transactional_properties"="insert_only");

EXPLAIN LOCKS MSCK TABLE repairtable_n6;
MSCK TABLE repairtable_n6;

show partitions repairtable_n6;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_n6/p1=a/p2=b/;
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_n6/p1=c/p2=d/;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_n6/p1=a/p2=b/datafile;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_n6/p1=c/p2=d/datafile;

EXPLAIN LOCKS MSCK REPAIR TABLE default.repairtable_n6;
MSCK REPAIR TABLE default.repairtable_n6;

show partitions default.repairtable_n6;

set hive.mapred.mode=strict;

dfs -rmr ${system:test.local.warehouse.dir}/repairtable_n6/p1=c;

EXPLAIN LOCKS MSCK REPAIR TABLE default.repairtable_n6 DROP PARTITIONS;
MSCK REPAIR TABLE default.repairtable_n6 DROP PARTITIONS;

show partitions default.repairtable_n6;

DROP TABLE default.repairtable_n6;
