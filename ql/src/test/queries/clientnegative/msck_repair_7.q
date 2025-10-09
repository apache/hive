DROP TABLE IF EXISTS repairtable;

CREATE TABLE repairtable(col STRING) PARTITIONED BY (p1 STRING);

dfs ${system:test.dfs.mkdir} ${hiveconf:hive.metastore.warehouse.dir}/repairtable/p1a;

MSCK REPAIR TABLE default.repairtable;