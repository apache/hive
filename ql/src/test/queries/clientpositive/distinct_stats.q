--! qt:dataset:src
set hive.stats.autogather=true;

set hive.compute.query.using.stats=true;
create table t1_n11 (a string, b string);

insert into table t1_n11 select * from src;

analyze table t1_n11 compute statistics for columns a,b;

explain 
select count(distinct b) from t1_n11 group by a;

explain 
select distinct(b) from t1_n11;

explain 
select a, count(*) from t1_n11 group by a;

drop table t1_n11;
set hive.compute.query.using.stats = false;
