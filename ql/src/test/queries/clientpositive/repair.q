DROP TABLE IF EXISTS repairtable_n4;

CREATE TABLE repairtable_n4(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);

MSCK TABLE repairtable_n4;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_n4/p1=a/p2=a;
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_n4/p1=b/p2=a;
dfs -touchz ${system:test.local.warehouse.dir}/repairtable_n4/p1=b/p2=a/datafile;

MSCK TABLE default.repairtable_n4;

MSCK REPAIR TABLE default.repairtable_n4;

MSCK TABLE repairtable_n4;

DROP TABLE default.repairtable_n4;
