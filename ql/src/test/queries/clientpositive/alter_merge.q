set hive.strict.checks.bucketing=false;

set hive.mapred.mode=nonstrict;
create table src_rc_merge_test_n2(key int, value string) stored as rcfile;

load data local inpath '../../data/files/smbbucket_1.rc' into table src_rc_merge_test_n2;
load data local inpath '../../data/files/smbbucket_2.rc' into table src_rc_merge_test_n2;
load data local inpath '../../data/files/smbbucket_3.rc' into table src_rc_merge_test_n2;

show table extended like `src_rc_merge_test_n2`;

select count(1) from src_rc_merge_test_n2;
select sum(hash(key)), sum(hash(value)) from src_rc_merge_test_n2;

alter table src_rc_merge_test_n2 concatenate;

show table extended like `src_rc_merge_test_n2`;

select count(1) from src_rc_merge_test_n2;
select sum(hash(key)), sum(hash(value)) from src_rc_merge_test_n2;


create table src_rc_merge_test_part_n0(key int, value string) partitioned by (ds string) stored as rcfile;

alter table src_rc_merge_test_part_n0 add partition (ds='2011');

load data local inpath '../../data/files/smbbucket_1.rc' into table src_rc_merge_test_part_n0 partition (ds='2011');
load data local inpath '../../data/files/smbbucket_2.rc' into table src_rc_merge_test_part_n0 partition (ds='2011');
load data local inpath '../../data/files/smbbucket_3.rc' into table src_rc_merge_test_part_n0 partition (ds='2011');

show table extended like `src_rc_merge_test_part_n0` partition (ds='2011');

select count(1) from src_rc_merge_test_part_n0;
select sum(hash(key)), sum(hash(value)) from src_rc_merge_test_part_n0;

alter table src_rc_merge_test_part_n0 partition (ds='2011') concatenate;

show table extended like `src_rc_merge_test_part_n0` partition (ds='2011');

select count(1) from src_rc_merge_test_part_n0;
select sum(hash(key)), sum(hash(value)) from src_rc_merge_test_part_n0;

drop table src_rc_merge_test_n2;
drop table src_rc_merge_test_part_n0;