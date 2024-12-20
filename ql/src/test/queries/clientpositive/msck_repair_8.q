DROP TABLE IF EXISTS tbl1;

CREATE EXTERNAL TABLE tbl1 (id INT, name STRING) PARTITIONED BY (my_date BIGINT) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/tbl1/';

dfs ${system:test.dfs.mkdir} -p ${system:test.tmp.dir}/apps/hive/warehouse/test.db/tbl1/my_date=__HIVE_DEFAULT_PARTITION__;

MSCK REPAIR TABLE tbl1;

show partitions tbl1;

DROP TABLE tbl1;
