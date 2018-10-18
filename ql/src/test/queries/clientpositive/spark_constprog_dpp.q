set hive.mapred.mode=nonstrict;
set hive.optimize.constant.propagation=true;
set hive.spark.dynamic.partition.pruning=true;

drop table if exists tb1_n0;
create table tb1_n0 (id int);

drop table if exists tb2_n0;
create table tb2_n0 (id smallint);

explain
select a.id from tb1_n0 a
left outer join
(select id from tb2_n0
union all
select 2 as id from tb2_n0 limit 1) b
on a.id=b.id;
