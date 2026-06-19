set hive.exec.dynamic.partition.mode=nonstrict;
set hive.fetch.task.conversion=none;
set hive.explain.user=false;

drop table if exists ice_01;
create external table ice_01 (key string, value string) partitioned by (ds string) stored by iceberg;

insert into ice_01 partition (ds) select 'A', 'V1', '2000-04-08';
insert into ice_01 partition (ds) select 'B', 'V2', 'null';
insert into ice_01 partition (ds) select 'C', 'V3', null;

explain select key, value, ds from ice_01 where ds is null;
select key, value, ds from ice_01 where ds is null;

explain select key, value, ds from ice_01 where ds is not null;
select key, value, ds from ice_01 where ds is not null order by key;

select key, value, ds from ice_01 where ds = 'null';