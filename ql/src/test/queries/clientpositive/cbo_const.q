set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;

select
  interval_day_time('2 1:2:3'),
  interval_day_time(cast('2 1:2:3' as string)),
  interval_day_time(cast('2 1:2:3' as varchar(10))),
  interval_day_time(cast('2 1:2:3' as char(10))),
  interval_day_time('2 1:2:3') = interval '2 1:2:3' day to second
from src limit 1;

select count(*) from srcpart join (select ds as ds, ds as `date` from srcpart group by ds) s on (srcpart.ds = s.ds) where s.`date` = '2008-04-08';

drop view t1;

create table t1_new (key string, value string) partitioned by (ds string);

insert overwrite table t1_new partition (ds = '2011-10-15')
select 'key1', 'value1' from src tablesample (1 rows);

insert overwrite table t1_new partition (ds = '2011-10-16')
select 'key2', 'value2' from src tablesample (1 rows);

create view t1 partitioned on (ds) as
select * from
(
select key, value, ds from t1_new
union all
select key, value, ds from t1_new
)subq;

select * from t1 where ds = '2011-10-15';


explain select array(1,2,3) from src;

EXPLAIN
select key from (SELECT key from src where key = 1+3)s;

select * from (select key from src where key = '1')subq;

select '1';

select * from (select '1')subq;

select * from (select key from src where false)subq;

EXPLAIN
SELECT x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key and y.key = 1+2)
JOIN srcpart z ON (x.value = z.value and z.ds='2008-04-08' and z.hr=11+3);

