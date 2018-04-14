--! qt:dataset:cbo_t2
--! qt:dataset:cbo_t1
set hive.mapred.mode=nonstrict;
set hive.exec.check.crossproducts=false;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- 10. Test views
create view v1_n18 as select c_int, value, c_boolean, dt from cbo_t1;
create view v2_n11 as select c_int, value from cbo_t2;

set hive.cbo.returnpath.hiveop=true;
select value from v1_n18 where c_boolean=false;
select max(c_int) from v1_n18 group by (c_boolean);

select count(v1_n18.c_int)  from v1_n18 join cbo_t2 on v1_n18.c_int = cbo_t2.c_int;
select count(v1_n18.c_int)  from v1_n18 join v2_n11 on v1_n18.c_int = v2_n11.c_int;

select count(*) from v1_n18 a join v1_n18 b on a.value = b.value;

set hive.cbo.returnpath.hiveop=false;
create view v3_n4 as select v1_n18.value val from v1_n18 join cbo_t1 on v1_n18.c_boolean = cbo_t1.c_boolean;

set hive.cbo.returnpath.hiveop=true;
select count(val) from v3_n4 where val != '1';
with q1 as ( select key from cbo_t1 where key = '1')
select count(*) from q1;

with q1 as ( select value from v1_n18 where c_boolean = false)
select count(value) from q1 ;

set hive.cbo.returnpath.hiveop=false;
create view v4_n4 as
with q1 as ( select key,c_int from cbo_t1  where key = '1')
select * from q1
;

set hive.cbo.returnpath.hiveop=true;
with q1 as ( select c_int from q2 where c_boolean = false),
q2 as ( select c_int,c_boolean from v1_n18  where value = '1')
select sum(c_int) from (select c_int from q1) a;

with q1 as ( select cbo_t1.c_int c_int from q2 join cbo_t1 where q2.c_int = cbo_t1.c_int  and cbo_t1.dt='2014'),
q2 as ( select c_int,c_boolean from v1_n18  where value = '1' or dt = '14')
select count(*) from q1 join q2 join v4_n4 on q1.c_int = q2.c_int and v4_n4.c_int = q2.c_int;


drop view v1_n18;
drop view v2_n11;
drop view v3_n4;
drop view v4_n4;
