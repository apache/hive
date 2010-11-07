drop table pcr_t1;
drop table pcr_t2;
drop table pcr_t3;

create table pcr_t1 (key int, value string) partitioned by (ds string);

insert overwrite table pcr_t1 partition (ds='2000-04-08') select * from src where key < 20 order by key;
insert overwrite table pcr_t1 partition (ds='2000-04-09') select * from src where key < 20 order by key;
insert overwrite table pcr_t1 partition (ds='2000-04-10') select * from src where key < 20 order by key;

explain extended select key, value, ds from pcr_t1 where ds<='2000-04-09' and key<5 order by key, ds;
select key, value, ds from pcr_t1 where ds<='2000-04-09' and key<5 order by key, ds;

explain extended select key, value from pcr_t1 where ds<='2000-04-09' or key<5 order by key;
select key, value from pcr_t1 where ds<='2000-04-09' or key<5 order by key;

explain extended select key, value, ds from pcr_t1 where ds<='2000-04-09' and key<5 and value != 'val_2' order by key, ds;
select key, value, ds from pcr_t1 where ds<='2000-04-09' and key<5 and value != 'val_2' order by key, ds;


explain extended
select key, value, ds from pcr_t1
where (ds < '2000-04-09' and key < 5) or (ds > '2000-04-09' and value == 'val_5') order by key, ds;

select key, value, ds from pcr_t1
where (ds < '2000-04-09' and key < 5) or (ds > '2000-04-09' and value == 'val_5') order by key, ds;


explain extended
select key, value, ds from pcr_t1
where (ds < '2000-04-10' and key < 5) or (ds > '2000-04-08' and value == 'val_5') order by key, ds;

select key, value, ds from pcr_t1
where (ds < '2000-04-10' and key < 5) or (ds > '2000-04-08' and value == 'val_5') order by key, ds;


explain extended
select key, value, ds from pcr_t1
where (ds < '2000-04-10' or key < 5) and (ds > '2000-04-08' or value == 'val_5') order by key, ds;

select key, value, ds from pcr_t1
where (ds < '2000-04-10' or key < 5) and (ds > '2000-04-08' or value == 'val_5') order by key, ds;


explain extended select key, value from pcr_t1 where (ds='2000-04-08' or ds='2000-04-09') and key=14 order by key, value;
select key, value from pcr_t1 where (ds='2000-04-08' or ds='2000-04-09') and key=14 order by key, value;

explain extended select key, value from pcr_t1 where ds='2000-04-08' or ds='2000-04-09' order by key, value;
select key, value from pcr_t1 where ds='2000-04-08' or ds='2000-04-09' order by key, value;

explain extended select key, value from pcr_t1 where ds>='2000-04-08' or ds<'2000-04-10' order by key, value;
select key, value from pcr_t1 where ds>='2000-04-08' or ds<'2000-04-10' order by key, value;

explain extended select key, value, ds from pcr_t1 where (ds='2000-04-08' and key=1) or (ds='2000-04-09' and key=2) order by key, value, ds;
select key, value, ds from pcr_t1 where (ds='2000-04-08' and key=1) or (ds='2000-04-09' and key=2) order by key, value, ds;

explain extended select * from pcr_t1 t1 join pcr_t1 t2 on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-08' order by t1.key;
select * from pcr_t1 t1 join pcr_t1 t2 on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-08' order by t1.key;

explain extended select * from pcr_t1 t1 join pcr_t1 t2 on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-09' order by t1.key;
select * from pcr_t1 t1 join pcr_t1 t2 on t1.key=t2.key and t1.ds='2000-04-08' and t2.ds='2000-04-09' order by t1.key;

insert overwrite table pcr_t1 partition (ds='2000-04-11') select * from src where key < 20 order by key;

explain extended select key, value, ds from pcr_t1 where (ds>'2000-04-08' and ds<'2000-04-11') or (ds>='2000-04-08' and ds<='2000-04-11' and key=2) order by key, value, ds;
select key, value, ds from pcr_t1 where (ds>'2000-04-08' and ds<'2000-04-11') or (ds>='2000-04-08' and ds<='2000-04-11' and key=2) order by key, value, ds;

explain extended select key, value, ds from pcr_t1 where (ds>'2000-04-08' and ds<'2000-04-11') or (ds<='2000-04-09' and key=2) order by key, value, ds;
select key, value, ds from pcr_t1 where (ds>'2000-04-08' and ds<'2000-04-11') or (ds<='2000-04-09' and key=2) order by key, value, ds;

create table pcr_t2 (key int, value string);
create table pcr_t3 (key int, value string);

explain extended
from pcr_t1
insert overwrite table pcr_t2 select key, value where ds='2000-04-08'
insert overwrite table pcr_t3 select key, value where ds='2000-04-08';

from pcr_t1
insert overwrite table pcr_t2 select key, value where ds='2000-04-08'
insert overwrite table pcr_t3 select key, value where ds='2000-04-08';

explain extended
from pcr_t1
insert overwrite table pcr_t2 select key, value where ds='2000-04-08' and key=2
insert overwrite table pcr_t3 select key, value where ds='2000-04-08' and key=3;

from pcr_t1
insert overwrite table pcr_t2 select key, value where ds='2000-04-08' and key=2
insert overwrite table pcr_t3 select key, value where ds='2000-04-08' and key=3;


explain extended select key, value from srcpart where ds='2008-04-08' and hr=11 order by key limit 10;
select key, value from srcpart where ds='2008-04-04' and hr=11 order by key limit 10;

explain extended select key, value, ds, hr from srcpart where ds='2008-04-08' and (hr='11' or hr='12') and key=11 order by key, ds, hr;
select key, value, ds, hr from srcpart where ds='2008-04-08' and (hr='11' or hr='12') and key=11 order by key, ds, hr;

explain extended select key, value, ds, hr from srcpart where hr='11' and key=11 order by key, ds, hr;
select key, value, ds, hr from srcpart where hr='11' and key=11 order by key, ds, hr;

drop table pcr_t1;
drop table pcr_t2;
drop table pcr_t3;
