DROP TABLE IF EXISTS repairtable;

CREATE TABLE repairtable(col STRING) PARTITIONED BY (p1 STRING);

MSCK REPAIR TABLE default.repairtable;

dfs ${system:test.dfs.mkdir} ${hiveconf:hive.metastore.warehouse.dir}/repairtable/p1=a;

MSCK REPAIR TABLE default.repairtable;

dfs ${system:test.dfs.mkdir} ${hiveconf:hive.metastore.warehouse.dir}/repairtable/P1=a;

MSCK REPAIR TABLE default.repairtable;

DROP TABLE default.repairtable;