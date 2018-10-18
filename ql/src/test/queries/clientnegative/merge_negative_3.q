--! qt:dataset:src



create table srcpart2 (key int, value string) partitioned by (ds string) clustered by (key) sorted by (key) into 2 buckets stored as RCFILE;
insert overwrite table srcpart2 partition (ds='2011') select * from src;
alter table srcpart2 partition (ds = '2011') concatenate;
