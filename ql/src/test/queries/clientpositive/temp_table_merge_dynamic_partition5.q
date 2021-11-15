--! qt:dataset:srcpart
SET hive.vectorized.execution.enabled=false;
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
-- this is to test the case where some dynamic partitions are merged and some are moved

create temporary table srcpart_merge_dp_temp like srcpart;

create temporary table srcpart_merge_dp_rc_temp like srcpart;
alter table srcpart_merge_dp_rc_temp set fileformat RCFILE;

create temporary table merge_dynamic_part_temp like srcpart;
alter table merge_dynamic_part_temp set fileformat RCFILE;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcpart_merge_dp_temp partition(ds='2008-04-08', hr=11);
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE srcpart_merge_dp_temp partition(ds='2008-04-08', hr=11);
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcpart_merge_dp_temp partition(ds='2008-04-08', hr=11);
load data local inpath '../../data/files/srcbucket23.txt' INTO TABLE srcpart_merge_dp_temp partition(ds='2008-04-08', hr=11);

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcpart_merge_dp_temp partition(ds='2008-04-08', hr=12);

insert overwrite table srcpart_merge_dp_rc_temp partition (ds = '2008-04-08', hr)
  select key, value, hr from srcpart_merge_dp_temp where ds = '2008-04-08';

set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=200;
set hive.exec.compress.output=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

explain
insert overwrite table merge_dynamic_part_temp partition (ds = '2008-04-08', hr)
  select key, value, if(key % 100 == 0, 'a1', 'b1') as hr from srcpart_merge_dp_rc_temp where ds = '2008-04-08';

insert overwrite table merge_dynamic_part_temp partition (ds = '2008-04-08', hr)
  select key, value, if(key % 100 == 0, 'a1', 'b1') as hr from srcpart_merge_dp_rc_temp where ds = '2008-04-08';

show partitions merge_dynamic_part_temp;

select count(*) from merge_dynamic_part_temp;
