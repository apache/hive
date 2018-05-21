--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.sparkfiles=true;
set mapred.min.split.size=256;
set mapred.min.split.size.per.node=256;
set mapred.min.split.size.per.rack=256;
set mapred.max.split.size=256;

-- SORT_QUERY_RESULTS

create table test1_n10(key int, val int);

explain
insert overwrite table test1_n10
select key, count(1) from src group by key;

insert overwrite table test1_n10
select key, count(1) from src group by key;

select * from test1_n10;

drop table test1_n10;


create table test_src_n0(key string, value string) partitioned by (ds string);
create table test1_n10(key string);

insert overwrite table test_src_n0 partition(ds='101') select * from src; 
insert overwrite table test_src_n0 partition(ds='102') select * from src;

explain 
insert overwrite table test1_n10 select key from test_src_n0;
insert overwrite table test1_n10 select key from test_src_n0;

set hive.merge.smallfiles.avgsize=16;
explain
insert overwrite table test1_n10 select key from test_src_n0;
insert overwrite table test1_n10 select key from test_src_n0;
