DROP TABLE IF EXISTS repairtable_n6;

CREATE EXTERNAL TABLE repairtable_n6(key INT, value STRING) PARTITIONED BY (Year INT, Month INT, Day INT) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/Repairtable_n6/';

MSCK REPAIR TABLE repairtable_n6;
show partitions repairtable_n6;

dfs  ${system:test.dfs.mkdir} -p ${system:test.tmp.dir}/apps/hive/warehouse/test.db/Repairtable_n6/Year=2020/Month=4/Day=1;

MSCK REPAIR TABLE repairtable_n6;
show partitions repairtable_n6;

DROP TABLE default.repairtable_n6;
