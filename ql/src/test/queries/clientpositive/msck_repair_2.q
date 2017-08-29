set hive.msck.repair.batch.size=1;
set hive.msck.path.validation=skip;

DROP TABLE IF EXISTS repairtable;

CREATE TABLE repairtable(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);

MSCK TABLE repairtable;

show partitions repairtable;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=c/p2=a/p3=b;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=c/p2=a/p3=b/datafile;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=c/datafile;

MSCK TABLE default.repairtable;
show partitions repairtable;

MSCK REPAIR TABLE default.repairtable;
show partitions repairtable;

MSCK TABLE repairtable;
show partitions repairtable;

DROP TABLE default.repairtable;
