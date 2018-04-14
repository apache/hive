--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.exec.submitviachild=false;
set hive.exec.submit.local.task.via.child=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=300;
set mapred.min.split.size=300;
set mapred.min.split.size.per.node=300;
set mapred.min.split.size.per.rack=300;
set hive.exec.mode.local.auto=true;
set hive.merge.smallfiles.avgsize=1;
set hive.compute.query.using.stats=true;

-- EXCLUDE_HADOOP_MAJOR_VERSIONS( 0.20S)

-- create file inputs
create table sih_i_part_n1 (key int, value string) partitioned by (p string);
insert overwrite table sih_i_part_n1 partition (p='1') select key, value from src;
insert overwrite table sih_i_part_n1 partition (p='2') select key+10000, value from src;
insert overwrite table sih_i_part_n1 partition (p='3') select key+20000, value from src;
create table sih_src_n1 as select key, value from sih_i_part_n1 order by key, value;
create table sih_src2_n1 as select key, value from sih_src_n1 order by key, value;

set hive.exec.post.hooks = org.apache.hadoop.hive.ql.hooks.VerifyIsLocalModeHook;
set mapreduce.framework.name=yarn;
set mapreduce.jobtracker.address=localhost:58;
set hive.sample.seednumber=7;

-- Relaxing hive.exec.mode.local.auto.input.files.max=1.
-- Hadoop20 will not generate more splits than there are files (one).
-- Hadoop23 generate splits correctly (four), hence the max needs to be adjusted to ensure running in local mode.
-- Default value is hive.exec.mode.local.auto.input.files.max=4 which produces expected behavior on Hadoop23.
-- hive.sample.seednumber is required because Hadoop23 generates multiple splits and tablesample is non-repeatable without it.

-- sample split, running locally limited by num tasks

desc formatted sih_src_n1;

explain select count(1) from sih_src_n1;

select count(1) from sih_src_n1;

explain select count(1) from sih_src_n1 tablesample(1 percent);

select count(1) from sih_src_n1 tablesample(1 percent);

explain select count(1) from sih_src_n1 tablesample(10 rows);

select count(1) from sih_src_n1 tablesample(10 rows);
