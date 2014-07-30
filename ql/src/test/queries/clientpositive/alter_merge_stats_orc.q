set hive.exec.submitviachild=true;
set hive.exec.submit.local.task.via.child=true;
create table src_orc_merge_test_stat(key int, value string) stored as orc;

insert overwrite table src_orc_merge_test_stat select * from src;
insert into table src_orc_merge_test_stat select * from src;
insert into table src_orc_merge_test_stat select * from src;

show table extended like `src_orc_merge_test_stat`;
desc extended src_orc_merge_test_stat;

analyze table src_orc_merge_test_stat compute statistics noscan;
desc formatted  src_orc_merge_test_stat;

alter table src_orc_merge_test_stat concatenate;
analyze table src_orc_merge_test_stat compute statistics noscan;
desc formatted src_orc_merge_test_stat;


create table src_orc_merge_test_part_stat(key int, value string) partitioned by (ds string) stored as orc;

alter table src_orc_merge_test_part_stat add partition (ds='2011');

insert overwrite table src_orc_merge_test_part_stat partition (ds='2011') select * from src;
insert into table src_orc_merge_test_part_stat partition (ds='2011') select * from src;
insert into table src_orc_merge_test_part_stat partition (ds='2011') select * from src;

show table extended like `src_orc_merge_test_part_stat` partition (ds='2011');
desc formatted src_orc_merge_test_part_stat partition (ds='2011');

analyze table src_orc_merge_test_part_stat partition(ds='2011') compute statistics noscan;
desc formatted src_orc_merge_test_part_stat partition (ds='2011');

alter table src_orc_merge_test_part_stat partition (ds='2011') concatenate;
analyze table src_orc_merge_test_part_stat partition(ds='2011') compute statistics noscan;
desc formatted src_orc_merge_test_part_stat partition (ds='2011');

drop table src_orc_merge_test_stat;
drop table src_orc_merge_test_part_stat;
