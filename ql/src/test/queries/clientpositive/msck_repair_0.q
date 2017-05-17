set hive.msck.repair.batch.size=1;
set hive.mv.files.thread=0;

DROP TABLE IF EXISTS repairtable;

CREATE TABLE repairtable(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);

MSCK TABLE repairtable;

show partitions repairtable;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=c/p2=a/p3=b;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=c/p2=a/p3=b/datafile;

MSCK TABLE default.repairtable;

show partitions default.repairtable;

MSCK REPAIR TABLE default.repairtable;

show partitions default.repairtable;

MSCK TABLE repairtable;

show partitions repairtable;

set hive.mapred.mode=strict;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=e/p2=f/p3=g;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=e/p2=f/p3=g/datafile;

MSCK REPAIR TABLE default.repairtable;

show partitions default.repairtable;

DROP TABLE default.repairtable;
