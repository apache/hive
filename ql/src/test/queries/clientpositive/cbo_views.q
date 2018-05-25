set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;
set hive.exec.check.crossproducts=false;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- 10. Test views
create view v1_n12 as select c_int, value, c_boolean, dt from cbo_t1;
create view v2_n6 as select c_int, value from cbo_t2;

select value from v1_n12 where c_boolean=false;
select max(c_int) from v1_n12 group by (c_boolean);

select count(v1_n12.c_int)  from v1_n12 join cbo_t2 on v1_n12.c_int = cbo_t2.c_int;
select count(v1_n12.c_int)  from v1_n12 join v2_n6 on v1_n12.c_int = v2_n6.c_int;

select count(*) from v1_n12 a join v1_n12 b on a.value = b.value;

create view v3_n2 as select v1_n12.value val from v1_n12 join cbo_t1 on v1_n12.c_boolean = cbo_t1.c_boolean;

select count(val) from v3_n2 where val != '1';
with q1 as ( select key from cbo_t1 where key = '1')
select count(*) from q1;

with q1 as ( select value from v1_n12 where c_boolean = false)
select count(value) from q1 ;

create view v4_n2 as
with q1 as ( select key,c_int from cbo_t1  where key = '1')
select * from q1
;

with q1 as ( select c_int from q2 where c_boolean = false),
q2 as ( select c_int,c_boolean from v1_n12  where value = '1')
select sum(c_int) from (select c_int from q1) a;

with q1 as ( select cbo_t1.c_int c_int from q2 join cbo_t1 where q2.c_int = cbo_t1.c_int  and cbo_t1.dt='2014'),
q2 as ( select c_int,c_boolean from v1_n12  where value = '1' or dt = '14')
select count(*) from q1 join q2 join v4_n2 on q1.c_int = q2.c_int and v4_n2.c_int = q2.c_int;


drop view v1_n12;
drop view v2_n6;
drop view v3_n2;
drop view v4_n2;
