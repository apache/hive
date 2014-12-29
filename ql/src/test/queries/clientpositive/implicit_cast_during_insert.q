create table implicit_cast_during_insert (c1 int, c2 string)
  partitioned by (p1 string) stored as orc;

set hive.exec.dynamic.partition.mode=nonstrict; 

explain 
insert overwrite table implicit_cast_during_insert partition (p1)
  select key, value, key key1 from (select * from src where key in (0,1)) q
  distribute by key1 sort by key1;

insert overwrite table implicit_cast_during_insert partition (p1)
  select key, value, key key1 from (select * from src where key in (0,1)) q
  distribute by key1 sort by key1;

select * from implicit_cast_during_insert;

drop table implicit_cast_during_insert;
