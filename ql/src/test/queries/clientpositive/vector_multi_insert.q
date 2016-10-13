set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

create table orc1
  stored as orc
  tblproperties("orc.compress"="ZLIB")
  as
    select rn
    from
    (
      select * from (select cast(1 as int) as rn from src limit 1)a
      union all
      select * from (select cast(100 as int) as rn from src limit 1)b
      union all
      select * from (select cast(10000 as int) as rn from src limit 1)c
    ) t;

create table orc_rn1 (rn int);
create table orc_rn2 (rn int);
create table orc_rn3 (rn int);

analyze table orc1 compute statistics;

explain vectorization from orc1 a
insert overwrite table orc_rn1 select a.* where a.rn < 100
insert overwrite table orc_rn2 select a.* where a.rn >= 100 and a.rn < 1000
insert overwrite table orc_rn3 select a.* where a.rn >= 1000;

from orc1 a
insert overwrite table orc_rn1 select a.* where a.rn < 100
insert overwrite table orc_rn2 select a.* where a.rn >= 100 and a.rn < 1000
insert overwrite table orc_rn3 select a.* where a.rn >= 1000;

select * from orc_rn1;
select * from orc_rn2;
select * from orc_rn3;
