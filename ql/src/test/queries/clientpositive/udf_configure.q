--! qt:dataset:src
SET hive.vectorized.execution.enabled=false;
set hive.test.vectorized.execution.enabled.override=disable;
--- This test attempts to verify HIVE-24645 which relies on fetch task conversion kicking in
set hive.fetch.task.conversion=more;
create temporary function testconfigure as 'org.apache.hadoop.hive.ql.udf.generic.TestConfigureUDF';
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
select *, testconfigure(key) from src limit 20;
