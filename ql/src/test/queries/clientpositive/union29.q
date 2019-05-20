--! qt:dataset:src
create table union_subq_union_n1(key int, value string);

explain
insert overwrite table union_subq_union_n1 
select * from (
  select key, value from src 
  union all 
  select key, value from 
  (
    select key, value from src 
    union all
    select key, value from src
  ) subq
) a
;

insert overwrite table union_subq_union_n1 
select * from (
  select key, value from src 
  union all 
  select key, value from 
  (
    select key, value from src 
    union all
    select key, value from src
  ) subq
) a
;

select * from union_subq_union_n1 order by key, value limit 20;
