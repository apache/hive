--! qt:dataset:src
create table t1 like src;
create table t2 like src;

explain from src
insert into table t1 select
key, GROUPING__ID
group by key, value with cube
insert into table t2 select
key, value
group by key, value grouping sets ((key), (key, value));