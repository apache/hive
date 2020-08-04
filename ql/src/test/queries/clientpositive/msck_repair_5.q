DROP TABLE IF EXISTS repairtable_n5;

CREATE EXTERNAL TABLE repairtable_n5(key INT, value STRING) PARTITIONED BY (Country String) stored as ORC;

MSCK REPAIR TABLE repairtable_n5;
show partitions repairtable_n5;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_n5/Country=US;
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_n5/Country=us;
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_n5/Country=India;

MSCK REPAIR TABLE repairtable_n5;
show partitions repairtable_n5;

DROP TABLE default.repairtable_n5;
