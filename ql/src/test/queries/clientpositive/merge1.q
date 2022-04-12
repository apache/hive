--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.merge.mapredfiles=true;

-- SORT_QUERY_RESULTS

create table dest1_n145(key int, val int);

explain
insert overwrite table dest1_n145
select key, count(1) from src group by key;

insert overwrite table dest1_n145
select key, count(1) from src group by key;

select * from dest1_n145;

drop table dest1_n145;

create table test_src_n2(key string, value string) partitioned by (ds string);
create table dest1_n145(key string);

insert overwrite table test_src_n2 partition(ds='101') select * from src; 
insert overwrite table test_src_n2 partition(ds='102') select * from src;

explain 
insert overwrite table dest1_n145 select key from test_src_n2;
insert overwrite table dest1_n145 select key from test_src_n2;

set hive.merge.smallfiles.avgsize=16;
explain
insert overwrite table dest1_n145 select key from test_src_n2;
insert overwrite table dest1_n145 select key from test_src_n2;
