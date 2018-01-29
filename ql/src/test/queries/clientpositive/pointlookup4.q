set hive.mapred.mode=nonstrict;
drop table pcr_t1;

create table pcr_t1 (key int, value string) partitioned by (ds1 string, ds2 string);
insert overwrite table pcr_t1 partition (ds1='2000-04-08', ds2='2001-04-08') select * from src where key < 20 order by key;
insert overwrite table pcr_t1 partition (ds1='2000-04-09', ds2='2001-04-09') select * from src where key < 20 order by key;
insert overwrite table pcr_t1 partition (ds1='2000-04-10', ds2='2001-04-10') select * from src where key < 20 order by key;

set hive.optimize.point.lookup=false;
set hive.optimize.partition.columns.separate=false;

explain extended
select key, value, ds1, ds2
from pcr_t1
where (ds1='2000-04-08' and ds2='2001-04-08' and key=1) or (ds1='2000-04-09' and ds2='2001-04-09' and key=2)
order by key, value, ds1, ds2;

select key, value, ds1, ds2
from pcr_t1
where (ds1='2000-04-08' and ds2='2001-04-08' and key=1) or (ds1='2000-04-09' and ds2='2001-04-09' and key=2)
order by key, value, ds1, ds2;

set hive.optimize.point.lookup=true;
set hive.optimize.point.lookup.min=0;
set hive.optimize.partition.columns.separate=true;

explain extended
select key, value, ds1, ds2
from pcr_t1
where (ds1='2000-04-08' and ds2='2001-04-08' and key=1) or (ds1='2000-04-09' and ds2='2001-04-09' and key=2)
order by key, value, ds1, ds2;

select key, value, ds1, ds2
from pcr_t1
where (ds1='2000-04-08' and ds2='2001-04-08' and key=1) or (ds1='2000-04-09' and ds2='2001-04-09' and key=2)
order by key, value, ds1, ds2;

drop table pcr_t1;
