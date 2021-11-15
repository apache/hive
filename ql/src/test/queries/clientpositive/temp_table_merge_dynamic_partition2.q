--! qt:dataset:srcpart
SET hive.vectorized.execution.enabled=false;
set hive.strict.checks.bucketing=false;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create temporary table srcpart_merge_dp_n0_temp like srcpart;

create temporary table merge_dynamic_part_n0_temp like srcpart;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcpart_merge_dp_n0_temp partition(ds='2008-04-08', hr=11);
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE srcpart_merge_dp_n0_temp partition(ds='2008-04-08', hr=11);
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcpart_merge_dp_n0_temp partition(ds='2008-04-08', hr=11);
load data local inpath '../../data/files/srcbucket23.txt' INTO TABLE srcpart_merge_dp_n0_temp partition(ds='2008-04-08', hr=11);
load data local inpath '../../data/files/srcbucket0.txt' INTO TABLE srcpart_merge_dp_n0_temp partition(ds='2008-04-08', hr=12);
load data local inpath '../../data/files/srcbucket1.txt' INTO TABLE srcpart_merge_dp_n0_temp partition(ds='2008-04-08', hr=12);


set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=3000;
set hive.exec.compress.output=false;

explain
insert overwrite table merge_dynamic_part_n0_temp partition (ds='2008-04-08', hr) select key, value, hr from srcpart_merge_dp_n0_temp where ds='2008-04-08';
insert overwrite table merge_dynamic_part_n0_temp partition (ds='2008-04-08', hr) select key, value, hr from srcpart_merge_dp_n0_temp where ds='2008-04-08';

show table extended like `merge_dynamic_part_n0_temp`;

