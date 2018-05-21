set hive.msck.repair.batch.size=1;
set hive.mv.files.thread=0;

DROP TABLE IF EXISTS repairtable_n5;

CREATE TABLE repairtable_n5(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);

MSCK TABLE repairtable_n5;

show partitions repairtable_n5;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=c/p2=a/p3=b;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=c/p2=a/p3=b/datafile;

MSCK TABLE default.repairtable_n5;

show partitions default.repairtable_n5;

MSCK REPAIR TABLE default.repairtable_n5;

show partitions default.repairtable_n5;

MSCK TABLE repairtable_n5;

show partitions repairtable_n5;

set hive.mapred.mode=strict;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=e/p2=f/p3=g;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=e/p2=f/p3=g/datafile;

MSCK REPAIR TABLE default.repairtable_n5;

show partitions default.repairtable_n5;

DROP TABLE default.repairtable_n5;
