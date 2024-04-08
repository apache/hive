SET metastore.expression.proxy=org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore;
SET metastore.decode.filter.expression.tostring=true;

DROP TABLE repairtable_string;
CREATE EXTERNAL TABLE repairtable_string(col STRING) PARTITIONED BY (p1 STRING, p2 STRING);
describe formatted repairtable_string;

alter table repairtable_string add partition (p1='1', p2='1');
alter table repairtable_string add partition (p1='1', p2='2');
alter table repairtable_string add partition (p1='2', p2='1');
alter table repairtable_string add partition (p1='2', p2='2');
alter table repairtable_string add partition (p1='3', p2='1');
alter table repairtable_string add partition (p1='3', p2='2');
show partitions default.repairtable_string;

dfs -rmr ${system:test.warehouse.dir}/repairtable_string/p1=1/p2=1;
MSCK REPAIR TABLE default.repairtable_string DROP PARTITIONS;
show partitions default.repairtable_string;
DROP TABLE repairtable_string;

SET metastore.integral.jdo.pushdown=true;
DROP TABLE repairtable_int;
CREATE EXTERNAL TABLE repairtable_int(col STRING) PARTITIONED BY (p1 INT, p2 INT);

alter table repairtable_int add partition (p1=1, p2=1);
alter table repairtable_int add partition (p1=1, p2=2);
alter table repairtable_int add partition (p1=2, p2=1);
alter table repairtable_int add partition (p1=2, p2=2);
alter table repairtable_int add partition (p1=3, p2=1);
alter table repairtable_int add partition (p1=3, p2=2);
show partitions default.repairtable_int;

dfs -rmr ${system:test.warehouse.dir}/repairtable_int/p1=1/p2=1;
MSCK REPAIR TABLE default.repairtable_int DROP PARTITIONS;
show partitions default.repairtable_int;
DROP TABLE repairtable_int;



SET metastore.integral.jdo.pushdown=false;
DROP TABLE repairtable_float;
CREATE EXTERNAL TABLE repairtable_float(col STRING) PARTITIONED BY (p1 FLOAT, p2 FLOAT);

alter table repairtable_float add partition (p1='1.0', p2='1.1');
alter table repairtable_float add partition (p1='1.0', p2='2.0');
alter table repairtable_float add partition (p1='2.0', p2='1.4');
alter table repairtable_float add partition (p1='2.0', p2='2.999');
alter table repairtable_float add partition (p1='3.0', p2='1.3');
alter table repairtable_float add partition (p1='3', p2='2');
show partitions default.repairtable_float;

dfs -rmr ${system:test.warehouse.dir}/repairtable_float/p1=1.0/p2=2.0;
MSCK REPAIR TABLE default.repairtable_float DROP PARTITIONS;
show partitions default.repairtable_float;
DROP TABLE repairtable_float;
