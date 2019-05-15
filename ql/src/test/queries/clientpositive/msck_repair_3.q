set hive.msck.repair.batch.size=1;

DROP TABLE IF EXISTS repairtable_n3;

CREATE TABLE repairtable_n3(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);

MSCK TABLE repairtable_n3;
show partitions repairtable_n3;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable_n3/p1=c/p2=a/p3=b;

MSCK TABLE default.repairtable_n3;
show partitions repairtable_n3;

MSCK REPAIR TABLE default.repairtable_n3;
show partitions repairtable_n3;

MSCK TABLE repairtable_n3;
show partitions repairtable_n3;

DROP TABLE default.repairtable_n3;
