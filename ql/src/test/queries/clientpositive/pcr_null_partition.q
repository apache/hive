set hive.exec.dynamic.partition.mode=nonstrict;
set hive.fetch.task.conversion=none;

drop table if exists pcr_t1;
create table pcr_t1 (key string, value string) partitioned by (ds string);

insert into pcr_t1 partition (ds) select 'A', 'V1', '2000-04-08';
insert into pcr_t1 partition (ds) select 'B', 'V2', 'null';
insert into pcr_t1 partition (ds) select 'C', 'V3', null;

explain select key, value, ds from pcr_t1 where ds is null;
select key, value, ds from pcr_t1 where ds is null;

explain select key, value, ds from pcr_t1 where ds is not null;
select key, value, ds from pcr_t1 where ds is not null order by key;

select key, value, ds from pcr_t1 where ds = 'null';
