--! qt:dataset:src
create table mytable (i integer, d date);

insert overwrite table mytable 
  select 1, cast('2011-01-01' as date) from src tablesample (1 rows);

select i, coalesce(d, cast(d as timestamp)) from mytable;

drop table mytable;
