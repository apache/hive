set hive.mapred.mode=nonstrict;
create table union_subq_union_n2(key int, value string);

explain
insert overwrite table union_subq_union_n2 
select * from (

select * from (
  select key, value from src 
  union all 
  select key, value from 
  (
    select key, value, count(1) from src group by key, value
    union all
    select key, value, count(1) from src group by key, value
  ) subq
) a

union all

select key, value from src
) aa
;

insert overwrite table union_subq_union_n2 
select * from (

select * from (
  select key, value from src 
  union all 
  select key, value from 
  (
    select key, value, count(1) from src group by key, value
    union all
    select key, value, count(1) from src group by key, value
  ) subq
) a

union all

select key, value from src
) aa
;

select * from union_subq_union_n2 order by key, value limit 20;
