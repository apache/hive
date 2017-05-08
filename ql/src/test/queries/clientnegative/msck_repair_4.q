set hive.msck.repair.batch.size=1;

DROP TABLE IF EXISTS repairtable;

CREATE TABLE repairtable(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);

MSCK TABLE repairtable;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p2=c/p1=a/p3=b;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p2=c/p1=a/p3=b/datafile;

MSCK REPAIR TABLE default.repairtable;

DROP TABLE default.repairtable;
