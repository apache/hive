DROP TABLE IF EXISTS repairtable_n7_1;
DROP TABLE IF EXISTS repairtable_n7_2;
DROP TABLE IF EXISTS repairtable_n7_3;
DROP TABLE IF EXISTS repairtable_n7_4;
DROP TABLE IF EXISTS repairtable_n7_5;
DROP TABLE IF EXISTS repairtable_n7_6;
DROP TABLE IF EXISTS repairtable_n7_7;
DROP TABLE IF EXISTS repairtable_n7_8;

CREATE EXTERNAL TABLE repairtable_n7_1(key INT) PARTITIONED BY (p1 TINYINT) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_1/';
CREATE EXTERNAL TABLE repairtable_n7_2(key INT) PARTITIONED BY (p1 SMALLINT) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_2/';
CREATE EXTERNAL TABLE repairtable_n7_3(key INT) PARTITIONED BY (p1 INT) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_3/';
CREATE EXTERNAL TABLE repairtable_n7_4(key INT) PARTITIONED BY (p1 BIGINT) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_4/';
CREATE EXTERNAL TABLE repairtable_n7_5(key INT) PARTITIONED BY (p1 FLOAT) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_5/';
CREATE EXTERNAL TABLE repairtable_n7_6(key INT) PARTITIONED BY (p1 DOUBLE) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_6/';
CREATE EXTERNAL TABLE repairtable_n7_7(key INT) PARTITIONED BY (p1 DECIMAL(10,10)) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_7/';
CREATE EXTERNAL TABLE repairtable_n7_8(key INT) PARTITIONED BY (p1 string) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_8/';

MSCK REPAIR TABLE repairtable_n7_1;
MSCK REPAIR TABLE repairtable_n7_2;
MSCK REPAIR TABLE repairtable_n7_3;
MSCK REPAIR TABLE repairtable_n7_4;
MSCK REPAIR TABLE repairtable_n7_5;
MSCK REPAIR TABLE repairtable_n7_6;
MSCK REPAIR TABLE repairtable_n7_7;
MSCK REPAIR TABLE repairtable_n7_8;

show partitions repairtable_n7_1;
show partitions repairtable_n7_2;
show partitions repairtable_n7_3;
show partitions repairtable_n7_4;
show partitions repairtable_n7_5;
show partitions repairtable_n7_6;
show partitions repairtable_n7_7;
show partitions repairtable_n7_8;

dfs  ${system:test.dfs.mkdir} -p ${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_1/p1=01;
dfs  ${system:test.dfs.mkdir} -p ${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_2/p1=01;
dfs  ${system:test.dfs.mkdir} -p ${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_3/p1=010;
dfs  ${system:test.dfs.mkdir} -p ${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_4/p1=-00100;
dfs  ${system:test.dfs.mkdir} -p ${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_5/p1=010.010;
dfs  ${system:test.dfs.mkdir} -p ${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_6/p1=-0100.00100;
dfs  ${system:test.dfs.mkdir} -p ${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_7/p1=01.00100;
dfs  ${system:test.dfs.mkdir} -p ${system:test.tmp.dir}/apps/hive/warehouse/test.db/repairtable_n7_8/p1=01;

MSCK REPAIR TABLE repairtable_n7_1;
MSCK REPAIR TABLE repairtable_n7_2;
MSCK REPAIR TABLE repairtable_n7_3;
MSCK REPAIR TABLE repairtable_n7_4;
MSCK REPAIR TABLE repairtable_n7_5;
MSCK REPAIR TABLE repairtable_n7_6;
MSCK REPAIR TABLE repairtable_n7_7;
MSCK REPAIR TABLE repairtable_n7_8;

show partitions repairtable_n7_1;
show partitions repairtable_n7_2;
show partitions repairtable_n7_3;
show partitions repairtable_n7_4;
show partitions repairtable_n7_5;
show partitions repairtable_n7_6;
show partitions repairtable_n7_7;
show partitions repairtable_n7_8;

DROP TABLE repairtable_n7_1;
DROP TABLE repairtable_n7_2;
DROP TABLE repairtable_n7_3;
DROP TABLE repairtable_n7_4;
DROP TABLE repairtable_n7_5;
DROP TABLE repairtable_n7_6;
DROP TABLE repairtable_n7_7;
DROP TABLE repairtable_n7_8;
