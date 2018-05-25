set hive.mapred.mode=nonstrict;
drop table pcr_t1_n2;
drop table pcr_t2_n0;
drop table pcr_t3;

create table pcr_t1_n2 (key int, value string) partitioned by (ds string);
insert overwrite table pcr_t1_n2 partition (ds='2000-04-08') select * from src where key < 20 order by key;
insert overwrite table pcr_t1_n2 partition (ds='2000-04-09') select * from src where key < 20 order by key;
insert overwrite table pcr_t1_n2 partition (ds='2000-04-10') select * from src where key < 20 order by key;

create table pcr_t2_n0 (ds string, key int, value string);
from pcr_t1_n2
insert overwrite table pcr_t2_n0 select ds, key, value where ds='2000-04-08';
from pcr_t1_n2
insert overwrite table pcr_t2_n0 select ds, key, value where ds='2000-04-08' and key=2;

explain extended
select key, value, ds
from pcr_t1_n2
where (ds='2000-04-08' and key=1) or (ds='2000-04-09' and key=2)
order by key, value, ds;

explain extended
select *
from pcr_t1_n2 t1 join pcr_t1_n2 t2
on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-08'
order by t1.key;

explain extended
select *
from pcr_t1_n2 t1 join pcr_t1_n2 t2
on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-09'
order by t1.key;

explain extended
select *
from pcr_t1_n2 t1 join pcr_t2_n0 t2
where (t1.ds='2000-04-08' and t2.key=1) or (t1.ds='2000-04-09' and t2.key=2)
order by t2.key, t2.value, t1.ds;

explain extended
select *
from pcr_t1_n2 t1 join pcr_t2_n0 t2
where (t2.ds='2000-04-08' and t1.key=1) or (t2.ds='2000-04-09' and t1.key=2)
order by t1.key, t1.value, t2.ds;

select key, value, ds
from pcr_t1_n2
where (ds='2000-04-08' and key=1) or (ds='2000-04-09' and key=2)
order by key, value, ds;

select *
from pcr_t1_n2 t1 join pcr_t1_n2 t2
on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-08'
order by t1.key;

select *
from pcr_t1_n2 t1 join pcr_t2_n0 t2
where (t1.ds='2000-04-08' and t2.key=1) or (t1.ds='2000-04-09' and t2.key=2)
order by t2.key, t2.value, t1.ds;

select *
from pcr_t1_n2 t1 join pcr_t2_n0 t2
where (t1.ds='2000-04-08' and t2.key=1) or (t1.ds='2000-04-09' and t2.key=2)
order by t2.key, t2.value, t1.ds;

select *
from pcr_t1_n2 t1 join pcr_t2_n0 t2
where (t2.ds='2000-04-08' and t1.key=1) or (t2.ds='2000-04-09' and t1.key=2)
order by t1.key, t1.value, t2.ds;

set hive.optimize.point.lookup.min=2;
set hive.optimize.partition.columns.separate=true;

explain extended
select key, value, ds
from pcr_t1_n2
where (ds='2000-04-08' and key=1) or (ds='2000-04-09' and key=2)
order by key, value, ds;

explain extended
select *
from pcr_t1_n2 t1 join pcr_t1_n2 t2
on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-08'
order by t1.key;

explain extended
select *
from pcr_t1_n2 t1 join pcr_t1_n2 t2
on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-09'
order by t1.key;

explain extended
select *
from pcr_t1_n2 t1 join pcr_t2_n0 t2
where (t1.ds='2000-04-08' and t2.key=1) or (t1.ds='2000-04-09' and t2.key=2)
order by t2.key, t2.value, t1.ds;

explain extended
select *
from pcr_t1_n2 t1 join pcr_t2_n0 t2
where (t2.ds='2000-04-08' and t1.key=1) or (t2.ds='2000-04-09' and t1.key=2)
order by t1.key, t1.value, t2.ds;

select key, value, ds
from pcr_t1_n2
where (ds='2000-04-08' and key=1) or (ds='2000-04-09' and key=2)
order by key, value, ds;

select *
from pcr_t1_n2 t1 join pcr_t1_n2 t2
on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-08'
order by t1.key;

select *
from pcr_t1_n2 t1 join pcr_t2_n0 t2
where (t1.ds='2000-04-08' and t2.key=1) or (t1.ds='2000-04-09' and t2.key=2)
order by t2.key, t2.value, t1.ds;

select *
from pcr_t1_n2 t1 join pcr_t2_n0 t2
where (t1.ds='2000-04-08' and t2.key=1) or (t1.ds='2000-04-09' and t2.key=2)
order by t2.key, t2.value, t1.ds;

select *
from pcr_t1_n2 t1 join pcr_t2_n0 t2
where (t2.ds='2000-04-08' and t1.key=1) or (t2.ds='2000-04-09' and t1.key=2)
order by t1.key, t1.value, t2.ds;

drop table pcr_t1_n2;
drop table pcr_t2_n0;
drop table pcr_t3;