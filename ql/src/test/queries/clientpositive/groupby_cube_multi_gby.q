--! qt:dataset:src
set hive.multigroupby.singlereducer=false;

create table t1_n21 like src;
create table t2_n13 like src;

explain from src
insert into table t1_n21 select
key, GROUPING__ID
group by cube(key, value)
insert into table t2_n13 select
key, value
group by key, value grouping sets ((key), (key, value));