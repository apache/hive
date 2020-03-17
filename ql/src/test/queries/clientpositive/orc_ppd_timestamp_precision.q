--! qt:dataset:src1
--! qt:dataset:src

set hive.vectorized.execution.enabled=false;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=5000;

create table tsstat (ts timestamp) stored as orc;
insert into tsstat values ("1970-01-01 00:00:00.0005");

set hive.optimize.index.filter=false;
select * from tsstat where ts = "1970-01-01 00:00:00.0005";

set hive.optimize.index.filter=true;
select * from tsstat where ts = "1970-01-01 00:00:00.0005";