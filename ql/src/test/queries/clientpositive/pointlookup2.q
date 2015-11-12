drop table pcr_t1;
drop table pcr_t2;
drop table pcr_t3;

create table pcr_t1 (key int, value string) partitioned by (ds string);
insert overwrite table pcr_t1 partition (ds='2000-04-08') select * from src where key < 20 order by key;
insert overwrite table pcr_t1 partition (ds='2000-04-09') select * from src where key < 20 order by key;
insert overwrite table pcr_t1 partition (ds='2000-04-10') select * from src where key < 20 order by key;

create table pcr_t2 (ds string, key int, value string);
from pcr_t1
insert overwrite table pcr_t2 select ds, key, value where ds='2000-04-08';
from pcr_t1
insert overwrite table pcr_t2 select ds, key, value where ds='2000-04-08' and key=2;

set hive.optimize.point.lookup.min=2;
set hive.optimize.partition.columns.separate=true;

explain extended
select key, value, ds
from pcr_t1
where (ds='2000-04-08' and key=1) or (ds='2000-04-09' and key=2)
order by key, value, ds;

explain extended
select *
from pcr_t1 t1 join pcr_t1 t2
on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-08'
order by t1.key;

explain extended
select *
from pcr_t1 t1 join pcr_t1 t2
on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-09'
order by t1.key;

explain extended
select *
from pcr_t1 t1 join pcr_t2 t2
where (t1.ds='2000-04-08' and t2.key=1) or (t1.ds='2000-04-09' and t2.key=2)
order by t2.key, t2.value, t1.ds;

explain extended
select *
from pcr_t1 t1 join pcr_t2 t2
where (t2.ds='2000-04-08' and t1.key=1) or (t2.ds='2000-04-09' and t1.key=2)
order by t1.key, t1.value, t2.ds;

drop table pcr_t1;
drop table pcr_t2;
drop table pcr_t3;