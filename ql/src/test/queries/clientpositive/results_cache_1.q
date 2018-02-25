
set hive.query.results.cache.enabled=true;

explain
select count(*) from src a join src b on (a.key = b.key);
select count(*) from src a join src b on (a.key = b.key);

set test.comment="Cache should be used for this query";
set test.comment;
explain
select count(*) from src a join src b on (a.key = b.key);
select count(*) from src a join src b on (a.key = b.key);

set hive.query.results.cache.enabled=false;
set test.comment="Cache is disabled, should not be used here.";
set test.comment;
explain
select count(*) from src a join src b on (a.key = b.key);

create database db1;
use db1;
create table src as select key, value from default.src;

set hive.query.results.cache.enabled=true;
set test.comment="Same query string, but different current database. Cache should not be used since unqualified tablenames resolve to different tables";
set test.comment;
explain
select count(*) from src a join src b on (a.key = b.key);

use default;

-- Union
select * from src where key = 0
union all
select * from src where key = 2;

set test.comment="Union all. Cache should be used now";
set test.comment;
explain
select * from src where key = 0
union all
select * from src where key = 2;

select * from src where key = 0
union all
select * from src where key = 2;


-- CTE
with q1 as ( select distinct key from q2 ),
q2 as ( select key, value from src where key < 10 )
select * from q1 a, q1 b where a.key = b.key;

set test.comment="CTE. Cache should be used now";
set test.comment;
explain
with q1 as ( select distinct key from q2 ),
q2 as ( select key, value from src where key < 10 )
select * from q1 a, q1 b where a.key = b.key;

with q1 as ( select distinct key from q2 ),
q2 as ( select key, value from src where key < 10 )
select * from q1 a, q1 b where a.key = b.key;

-- Intersect/Except
with q1 as ( select distinct key, value from src ),
q2 as ( select key, value from src where key < 10 ),
q3 as ( select key, value from src where key = 0 )
select * from q1 intersect all select * from q2 except all select * from q3;

set test.comment="Intersect/Except. Cache should be used now";
set test.comment;
explain
with q1 as ( select distinct key, value from src ),
q2 as ( select key, value from src where key < 10 ),
q3 as ( select key, value from src where key = 0 )
select * from q1 intersect all select * from q2 except all select * from q3;

with q1 as ( select distinct key, value from src ),
q2 as ( select key, value from src where key < 10 ),
q3 as ( select key, value from src where key = 0 )
select * from q1 intersect all select * from q2 except all select * from q3;

-- Semijoin. Use settings from cbo_semijoin
set hive.mapred.mode=nonstrict;
set hive.exec.check.crossproducts=false;
set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

select a, c, count(*)  from (select key as a, c_int+1 as b, sum(c_int) as c from cbo_t1 where (cbo_t1.c_int + 1 >= 0) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)  group by c_float, cbo_t1.c_int, key having cbo_t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by a+b desc, c asc limit 5) cbo_t1 left semi join (select key as p, c_int+1 as q, sum(c_int) as r from cbo_t2 where (cbo_t2.c_int + 1 >= 0) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)  group by c_float, cbo_t2.c_int, key having cbo_t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by q+r/10 desc, p limit 5) cbo_t2 on cbo_t1.a=p left semi join cbo_t3 on cbo_t1.a=key where (b + 1  >= 0) and (b > 0 or a >= 0) group by a, c  having a > 0 and (a >=1 or c >= 1) and (a + c) >= 0 order by c, a;

set test.comment="Semijoin. Cache should be used now";
set test.comment;
explain
select a, c, count(*)  from (select key as a, c_int+1 as b, sum(c_int) as c from cbo_t1 where (cbo_t1.c_int + 1 >= 0) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)  group by c_float, cbo_t1.c_int, key having cbo_t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by a+b desc, c asc limit 5) cbo_t1 left semi join (select key as p, c_int+1 as q, sum(c_int) as r from cbo_t2 where (cbo_t2.c_int + 1 >= 0) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)  group by c_float, cbo_t2.c_int, key having cbo_t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by q+r/10 desc, p limit 5) cbo_t2 on cbo_t1.a=p left semi join cbo_t3 on cbo_t1.a=key where (b + 1  >= 0) and (b > 0 or a >= 0) group by a, c  having a > 0 and (a >=1 or c >= 1) and (a + c) >= 0 order by c, a;
select a, c, count(*)  from (select key as a, c_int+1 as b, sum(c_int) as c from cbo_t1 where (cbo_t1.c_int + 1 >= 0) and (cbo_t1.c_int > 0 or cbo_t1.c_float >= 0)  group by c_float, cbo_t1.c_int, key having cbo_t1.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by a+b desc, c asc limit 5) cbo_t1 left semi join (select key as p, c_int+1 as q, sum(c_int) as r from cbo_t2 where (cbo_t2.c_int + 1 >= 0) and (cbo_t2.c_int > 0 or cbo_t2.c_float >= 0)  group by c_float, cbo_t2.c_int, key having cbo_t2.c_float > 0 and (c_int >=1 or c_float >= 1) and (c_int + c_float) >= 0 order by q+r/10 desc, p limit 5) cbo_t2 on cbo_t1.a=p left semi join cbo_t3 on cbo_t1.a=key where (b + 1  >= 0) and (b > 0 or a >= 0) group by a, c  having a > 0 and (a >=1 or c >= 1) and (a + c) >= 0 order by c, a;
