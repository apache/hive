set hive.mapred.mode=nonstrict;
set hive.optimize.constant.propagation=true;
set hive.spark.dynamic.partition.pruning=true;

drop table if exists tb1;
create table tb1 (id int);

drop table if exists tb2;
create table tb2 (id smallint);

explain
select a.id from tb1 a
left outer join
(select id from tb2
union all
select 2 as id from tb2 limit 1) b
on a.id=b.id;
