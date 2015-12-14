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

create table test1(key int, val int);

explain
insert overwrite table test1
select key, count(1) from src group by key;

insert overwrite table test1
select key, count(1) from src group by key;

select * from test1;

drop table test1;


create table test_src(key string, value string) partitioned by (ds string);
create table test1(key string);

insert overwrite table test_src partition(ds='101') select * from src; 
insert overwrite table test_src partition(ds='102') select * from src;

explain 
insert overwrite table test1 select key from test_src;
insert overwrite table test1 select key from test_src;

set hive.merge.smallfiles.avgsize=16;
explain
insert overwrite table test1 select key from test_src;
insert overwrite table test1 select key from test_src;
