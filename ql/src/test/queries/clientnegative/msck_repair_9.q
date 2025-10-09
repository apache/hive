DROP TABLE IF EXISTS tbl_int;

CREATE EXTERNAL TABLE tbl_int (id INT, name STRING) PARTITIONED BY (month INT) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/tbl_int/';
dfs ${system:test.dfs.mkdir} -p ${system:test.tmp.dir}/apps/hive/warehouse/test.db/tbl_int/month=random_partition;
MSCK REPAIR TABLE tbl_int;

DROP TABLE IF EXISTS tbl_int;
