--! qt:dataset:src
USE default;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=300;
set mapred.min.split.size=300;
set mapred.min.split.size.per.node=300;
set mapred.min.split.size.per.rack=300;
set hive.exec.mode.local.auto=true;
set hive.merge.smallfiles.avgsize=1;

-- INCLUDE_HADOOP_MAJOR_VERSIONS( 0.20S)
-- This test sets mapred.max.split.size=300 and hive.merge.smallfiles.avgsize=1
-- in an attempt to force the generation of multiple splits and multiple output files.
-- However, Hadoop 0.20 is incapable of generating splits smaller than the block size
-- when using CombineFileInputFormat, so only one split is generated. This has a
-- significant impact on the results of the TABLESAMPLE(x PERCENT). This issue was
-- fixed in MAPREDUCE-2046 which is included in 0.22.

-- create file inputs
create table sih_i_part_n0 (key int, value string) partitioned by (p string);
insert overwrite table sih_i_part_n0 partition (p='1') select key, value from src;
insert overwrite table sih_i_part_n0 partition (p='2') select key+10000, value from src;
insert overwrite table sih_i_part_n0 partition (p='3') select key+20000, value from src;
create table sih_src_n0 as select key, value from sih_i_part_n0 order by key, value;
create table sih_src2_n0 as select key, value from sih_src_n0 order by key, value;

set hive.exec.post.hooks = org.apache.hadoop.hive.ql.hooks.VerifyIsLocalModeHook ;
set mapred.job.tracker=localhost:58;
set hive.exec.mode.local.auto.input.files.max=1;

-- Sample split, running locally limited by num tasks
select count(1) from sih_src_n0 tablesample(1 percent);

-- sample two tables
select count(1) from sih_src_n0 tablesample(1 percent)a join sih_src2_n0 tablesample(1 percent)b on a.key = b.key;

set hive.exec.mode.local.auto.inputbytes.max=1000;
set hive.exec.mode.local.auto.input.files.max=4;

-- sample split, running locally limited by max bytes
select count(1) from sih_src_n0 tablesample(1 percent);
