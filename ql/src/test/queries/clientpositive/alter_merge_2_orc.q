--! qt:dataset:src
set hive.mapred.mode=nonstrict;
create table src_orc_merge_test_part(key int, value string) partitioned by (ds string, ts string) stored as orc;

alter table src_orc_merge_test_part add partition (ds='2012-01-03', ts='2012-01-03+14:46:31');
desc extended src_orc_merge_test_part partition (ds='2012-01-03', ts='2012-01-03+14:46:31');

insert overwrite table src_orc_merge_test_part partition(ds='2012-01-03', ts='2012-01-03+14:46:31') select * from src order by key, value;
insert into table src_orc_merge_test_part partition(ds='2012-01-03', ts='2012-01-03+14:46:31') select * from src order by key, value limit 100;
insert into table src_orc_merge_test_part partition(ds='2012-01-03', ts='2012-01-03+14:46:31') select * from src order by key, value limit 10;

select count(1) from src_orc_merge_test_part where ds='2012-01-03' and ts='2012-01-03+14:46:31';
select sum(hash(key)), sum(hash(value)) from src_orc_merge_test_part where ds='2012-01-03' and ts='2012-01-03+14:46:31';

alter table src_orc_merge_test_part partition (ds='2012-01-03', ts='2012-01-03+14:46:31') concatenate;


select count(1) from src_orc_merge_test_part where ds='2012-01-03' and ts='2012-01-03+14:46:31';
select sum(hash(key)), sum(hash(value)) from src_orc_merge_test_part where ds='2012-01-03' and ts='2012-01-03+14:46:31';

drop table src_orc_merge_test_part;
