--! qt:dataset:src
SET hive.vectorized.execution.enabled=false;
set hive.test.vectorized.execution.enabled.override=disable;

create temporary function counter as 'org.apache.hadoop.hive.ql.udf.generic.DummyContextUDF';

set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

select *, counter(key) from src limit 20;
