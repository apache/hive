--! qt:dataset:src
set hive.optimize.point.lookup.min=31;
set hive.mapred.mode=nonstrict;
drop table pcr_t1_n1;

create table pcr_t1_n1 (key int, value string) partitioned by (ds1 string, ds2 string);
insert overwrite table pcr_t1_n1 partition (ds1='2000-04-08', ds2='2001-04-08') select * from src where key < 20 order by key;
insert overwrite table pcr_t1_n1 partition (ds1='2000-04-09', ds2='2001-04-09') select * from src where key < 20 order by key;
insert overwrite table pcr_t1_n1 partition (ds1='2000-04-10', ds2='2001-04-10') select * from src where key < 20 order by key;

explain extended
select key, value, ds1, ds2
from pcr_t1_n1
where (ds1='2000-04-08' and key=1) or (ds1='2000-04-09' and key=2)
order by key, value, ds1, ds2;

explain extended
select key, value, ds1, ds2
from pcr_t1_n1
where (ds1='2000-04-08' and ds2='2001-04-08' and key=1) or (ds1='2000-04-09' and ds2='2001-04-08' and key=2)
order by key, value, ds1, ds2;

explain extended
select *
from pcr_t1_n1 t1 join pcr_t1_n1 t2
on t1.key=t2.key and t1.ds1='2000-04-08' and t2.ds2='2001-04-08'
order by t2.key, t2.value, t1.ds1;

explain extended
select *
from pcr_t1_n1 t1 join pcr_t1_n1 t2
on t1.key=t2.key and t1.ds1='2000-04-08' and t2.ds1='2000-04-09'
order by t2.key, t2.value, t1.ds1;

explain extended
select *
from pcr_t1_n1 t1 join pcr_t1_n1 t2
where (t1.ds1='2000-04-08' and t2.key=1) or (t1.ds1='2000-04-09' and t2.key=2)
order by t2.key, t2.value, t1.ds1;

select key, value, ds1, ds2
from pcr_t1_n1
where (ds1='2000-04-08' and key=1) or (ds1='2000-04-09' and key=2)
order by key, value, ds1, ds2;

select key, value, ds1, ds2
from pcr_t1_n1
where (ds1='2000-04-08' and ds2='2001-04-08' and key=1) or (ds1='2000-04-09' and ds2='2001-04-08' and key=2)
order by key, value, ds1, ds2;

select *
from pcr_t1_n1 t1 join pcr_t1_n1 t2
on t1.key=t2.key and t1.ds1='2000-04-08' and t2.ds2='2001-04-08'
order by t2.key, t2.value, t1.ds1;

select *
from pcr_t1_n1 t1 join pcr_t1_n1 t2
on t1.key=t2.key and t1.ds1='2000-04-08' and t2.ds1='2000-04-09'
order by t2.key, t2.value, t1.ds1;

select *
from pcr_t1_n1 t1 join pcr_t1_n1 t2
where (t1.ds1='2000-04-08' and t2.key=1) or (t1.ds1='2000-04-09' and t2.key=2)
order by t2.key, t2.value, t1.ds1;

set hive.optimize.point.lookup.min=2;
set hive.optimize.partition.columns.separate=true;

explain extended
select key, value, ds1, ds2
from pcr_t1_n1
where (ds1='2000-04-08' and key=1) or (ds1='2000-04-09' and key=2)
order by key, value, ds1, ds2;

explain extended
select key, value, ds1, ds2
from pcr_t1_n1
where (ds1='2000-04-08' and ds2='2001-04-08' and key=1) or (ds1='2000-04-09' and ds2='2001-04-08' and key=2)
order by key, value, ds1, ds2;

explain extended
select *
from pcr_t1_n1 t1 join pcr_t1_n1 t2
on t1.key=t2.key and t1.ds1='2000-04-08' and t2.ds2='2001-04-08'
order by t2.key, t2.value, t1.ds1;

explain extended
select *
from pcr_t1_n1 t1 join pcr_t1_n1 t2
on t1.key=t2.key and t1.ds1='2000-04-08' and t2.ds1='2000-04-09'
order by t2.key, t2.value, t1.ds1;

explain extended
select *
from pcr_t1_n1 t1 join pcr_t1_n1 t2
where (t1.ds1='2000-04-08' and t2.key=1) or (t1.ds1='2000-04-09' and t2.key=2)
order by t2.key, t2.value, t1.ds1;

select key, value, ds1, ds2
from pcr_t1_n1
where (ds1='2000-04-08' and key=1) or (ds1='2000-04-09' and key=2)
order by key, value, ds1, ds2;

select key, value, ds1, ds2
from pcr_t1_n1
where (ds1='2000-04-08' and ds2='2001-04-08' and key=1) or (ds1='2000-04-09' and ds2='2001-04-08' and key=2)
order by key, value, ds1, ds2;

select *
from pcr_t1_n1 t1 join pcr_t1_n1 t2
on t1.key=t2.key and t1.ds1='2000-04-08' and t2.ds2='2001-04-08'
order by t2.key, t2.value, t1.ds1;

select *
from pcr_t1_n1 t1 join pcr_t1_n1 t2
on t1.key=t2.key and t1.ds1='2000-04-08' and t2.ds1='2000-04-09'
order by t2.key, t2.value, t1.ds1;

select *
from pcr_t1_n1 t1 join pcr_t1_n1 t2
where (t1.ds1='2000-04-08' and t2.key=1) or (t1.ds1='2000-04-09' and t2.key=2)
order by t2.key, t2.value, t1.ds1;

drop table pcr_t1_n1;
