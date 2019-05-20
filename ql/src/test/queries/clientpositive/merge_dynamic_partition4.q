--! qt:dataset:srcpart
SET hive.vectorized.execution.enabled=false;
set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
-- this test verifies that the block merge task that can follow a query to generate dynamic
-- partitions does not produce incorrect results by dropping partitions

create table srcpart_merge_dp_n4 like srcpart;

create table srcpart_merge_dp_rc_n1 like srcpart;
alter table srcpart_merge_dp_rc_n1 set fileformat RCFILE;

create table merge_dynamic_part_n3 like srcpart;
alter table merge_dynamic_part_n3 set fileformat RCFILE;

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcpart_merge_dp_n4 partition(ds='2008-04-08', hr=11);
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE srcpart_merge_dp_n4 partition(ds='2008-04-08', hr=11);
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcpart_merge_dp_n4 partition(ds='2008-04-08', hr=11);
load data local inpath '../../data/files/srcbucket23.txt' INTO TABLE srcpart_merge_dp_n4 partition(ds='2008-04-08', hr=11);

load data local inpath '../../data/files/srcbucket20.txt' INTO TABLE srcpart_merge_dp_n4 partition(ds='2008-04-08', hr=12);
load data local inpath '../../data/files/srcbucket21.txt' INTO TABLE srcpart_merge_dp_n4 partition(ds='2008-04-08', hr=12);
load data local inpath '../../data/files/srcbucket22.txt' INTO TABLE srcpart_merge_dp_n4 partition(ds='2008-04-08', hr=12);
load data local inpath '../../data/files/srcbucket23.txt' INTO TABLE srcpart_merge_dp_n4 partition(ds='2008-04-08', hr=12);

insert overwrite table srcpart_merge_dp_rc_n1 partition (ds = '2008-04-08', hr) 
select key, value, hr from srcpart_merge_dp_n4 where ds = '2008-04-08';

set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=10000000000000;
set hive.exec.compress.output=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

explain
insert overwrite table merge_dynamic_part_n3 partition (ds = '2008-04-08', hr)
select key, value, if(key % 2 == 0, 'a1', 'b1') as hr from srcpart_merge_dp_rc_n1 where ds = '2008-04-08';

insert overwrite table merge_dynamic_part_n3 partition (ds = '2008-04-08', hr)
select key, value, if(key % 2 == 0, 'a1', 'b1') as hr from srcpart_merge_dp_rc_n1 where ds = '2008-04-08';

show partitions merge_dynamic_part_n3;

select count(*) from merge_dynamic_part_n3;
