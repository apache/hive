set hive.msck.repair.batch.size=1;
set hive.msck.path.validation=skip;

DROP TABLE IF EXISTS repairtable_n2;

CREATE TABLE repairtable_n2(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);

MSCK TABLE repairtable_n2;

show partitions repairtable_n2;

dfs ${system:test.dfs.mkdir} ${system:test.warehouse.dir}/repairtable/p1=c/p2=a/p3=b;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=c/p2=a/p3=b/datafile;
dfs -touchz ${system:test.warehouse.dir}/repairtable/p1=c/datafile;

MSCK TABLE default.repairtable_n2;
show partitions repairtable_n2;

MSCK REPAIR TABLE default.repairtable_n2;
show partitions repairtable_n2;

MSCK TABLE repairtable_n2;
show partitions repairtable_n2;

DROP TABLE default.repairtable_n2;
