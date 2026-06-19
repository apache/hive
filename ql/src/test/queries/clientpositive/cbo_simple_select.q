--! qt:dataset:cbo_t3
--! qt:dataset:cbo_t2
--! qt:dataset:cbo_t1
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;
set hive.exec.check.crossproducts=false;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- 1. Test Select + TS
select * from cbo_t1;
select * from cbo_t1 as cbo_t1;
select * from cbo_t1 as cbo_t2;

select cbo_t1.key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from cbo_t1;
select * from cbo_t1 where (((key=1) and (c_float=10)) and (c_int=20)); 

-- 2. Test Select + TS + FIL
select * from cbo_t1 where cbo_t1.c_int >= 0;
select * from cbo_t1 as cbo_t1  where cbo_t1.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;
select * from cbo_t1 as cbo_t2 where cbo_t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;

select cbo_t2.key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from cbo_t1 as cbo_t2  where cbo_t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;

-- 3 Test Select + Select + TS + FIL
select * from (select * from cbo_t1 where cbo_t1.c_int >= 0) as cbo_t1;
select * from (select * from cbo_t1 as cbo_t1  where cbo_t1.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as cbo_t1;
select * from (select * from cbo_t1 as cbo_t2 where cbo_t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as cbo_t1;
select * from (select cbo_t2.key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from cbo_t1 as cbo_t2  where cbo_t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as cbo_t1;

select * from (select * from cbo_t1 where cbo_t1.c_int >= 0) as cbo_t1 where cbo_t1.c_int >= 0;
select * from (select * from cbo_t1 as cbo_t1  where cbo_t1.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as cbo_t1  where cbo_t1.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;
select * from (select * from cbo_t1 as cbo_t2 where cbo_t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as cbo_t2 where cbo_t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;
select * from (select cbo_t2.key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from cbo_t1 as cbo_t2  where cbo_t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as cbo_t1 where cbo_t1.c_int >= 0 and y+c_int >= 0 or x <= 100;

select cbo_t1.c_int+c_float as x , c_int as c_int, (((c_int+c_float)*10)+5) as y from (select * from cbo_t1 where cbo_t1.c_int >= 0) as cbo_t1 where cbo_t1.c_int >= 0;
select cbo_t2.c_int+c_float as x , c_int as c_int, (((c_int+c_float)*10)+5) as y from (select * from cbo_t1 where cbo_t1.c_int >= 0) as cbo_t2 where cbo_t2.c_int >= 0;



select * from (select * from cbo_t1 where cbo_t1.c_int >= 0) as cbo_t1 where cbo_t1.c_int >= 0;
select * from (select * from cbo_t1 as cbo_t1  where cbo_t1.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as cbo_t1  where cbo_t1.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;
select * from (select * from cbo_t1 as cbo_t2 where cbo_t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as cbo_t2 where cbo_t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100;
select * from (select cbo_t2.key as x, c_int as c_int, (((c_int+c_float)*10)+5) as y from cbo_t1 as cbo_t2  where cbo_t2.c_int >= 0 and c_float+c_int >= 0 or c_float <= 100) as cbo_t1 where cbo_t1.c_int >= 0 and y+c_int >= 0 or x <= 100;

select cbo_t1.c_int+c_float as x , c_int as c_int, (((c_int+c_float)*10)+5) as y from (select * from cbo_t1 where cbo_t1.c_int >= 0) as cbo_t1 where cbo_t1.c_int >= 0;
select cbo_t2.c_int+c_float as x , c_int as c_int, (((c_int+c_float)*10)+5) as y from (select * from cbo_t1 where cbo_t1.c_int >= 0) as cbo_t2 where cbo_t2.c_int >= 0;



-- 13. null expr in select list
select null from cbo_t3;

-- 14. unary operator
select key from cbo_t1 where c_int = -6  or c_int = +6;

-- 15. query referencing only partition columns
select count(cbo_t1.dt) from cbo_t1 join cbo_t2 on cbo_t1.dt  = cbo_t2.dt  where cbo_t1.dt = '2014' ;


-- IN expression rewrite

EXPLAIN select * from cbo_t2 where (cbo_t2.c_int) IN (cbo_t2.c_int); -- c_int is not null
EXPLAIN select * from cbo_t2 where (cbo_t2.c_int) IN (2*cbo_t2.c_int); -- c_int is 0
EXPLAIN select * from cbo_t2 where (cbo_t2.c_int) = (cbo_t2.c_int); -- c_int is not null
EXPLAIN select * from cbo_t2 where (cbo_t2.c_int) IN (NULL); -- rewrite to NULL
EXPLAIN select * from cbo_t2 where (cbo_t2.c_int) IN (cbo_t2.c_int, 2*cbo_t2.c_int); -- no rewrite
EXPLAIN select * from cbo_t2 where (cbo_t2.c_int) IN (cbo_t2.c_int, 0); -- no rewrite

select count(*) from cbo_t2 where (cbo_t2.c_int) IN (cbo_t2.c_int); -- c_int is not null
select count(*) from cbo_t2 where (cbo_t2.c_int) IN (2*cbo_t2.c_int); -- c_int is 0
select count(*) from cbo_t2 where (cbo_t2.c_int) = (cbo_t2.c_int); -- c_int is not null
select count(*) from cbo_t2 where (cbo_t2.c_int) IN (NULL); -- rewrite to NULL
select count(*) from cbo_t2 where (cbo_t2.c_int) IN (cbo_t2.c_int, 2*cbo_t2.c_int); -- no rewrite
select count(*) from cbo_t2 where (cbo_t2.c_int) IN (cbo_t2.c_int, 0); -- no rewrite

