set hive.msck.repair.batch.size=1;

DROP TABLE IF EXISTS repairtable;

CREATE TABLE repairtable(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);

MSCK TABLE repairtable;

SHOW PARTITIONS repairtable;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=c/p2=a/p3=b;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=c/p2=a/p3=b/datafile;

MSCK TABLE default.repairtable;

SHOW PARTITIONS default.repairtable;

MSCK REPAIR TABLE default.repairtable;

SHOW PARTITIONS default.repairtable;

MSCK TABLE repairtable;

SHOW PARTITIONS repairtable;

DROP TABLE default.repairtable;
