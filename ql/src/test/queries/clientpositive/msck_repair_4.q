DROP TABLE IF EXISTS repairtable_n4;

CREATE EXTERNAL TABLE repairtable_n4(key INT, value STRING) PARTITIONED BY (Year INT, Month INT, Day INT) stored as ORC;

MSCK REPAIR TABLE repairtable_n4;
show partitions repairtable_n4;

dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_n4/Year=2020/Month=3/Day=1;
dfs ${system:test.dfs.mkdir} ${system:test.local.warehouse.dir}/repairtable_n4/Year=2020/Month=3/Day=2;

MSCK REPAIR TABLE repairtable_n4;
show partitions repairtable_n4;

DROP TABLE default.repairtable_n4;
