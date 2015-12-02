set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;
set hive.cbo.returnpath.hiveop=true;
set hive.exec.check.crossproducts=false;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- SORT_QUERY_RESULTS
-- Merge join into multijoin operator 1
explain select key, cbo_t1.c_int, cbo_t2.p, q from cbo_t1 join
(select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2) cbo_t2 on cbo_t1.key=p right outer join
(select key as a, c_int as b, cbo_t3.c_float as c from cbo_t3) cbo_t3 on cbo_t1.key=a;

select key, cbo_t1.c_int, cbo_t2.p, q from cbo_t1 join
(select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2) cbo_t2 on cbo_t1.key=p right outer join
(select key as a, c_int as b, cbo_t3.c_float as c from cbo_t3) cbo_t3 on cbo_t1.key=a;

-- Merge join into multijoin operator 2
explain select key, c_int, cbo_t2.p, cbo_t2.q, cbo_t3.x, cbo_t4.b from cbo_t1 join
(select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2) cbo_t2 on cbo_t1.key=p right outer join
(select cbo_t3.key as x, cbo_t3.c_int as y, c_float as z from cbo_t3) cbo_t3 on cbo_t1.key=x left outer join
(select key as a, c_int as b, c_float as c from cbo_t1) cbo_t4 on cbo_t1.key=a;

select key, c_int, cbo_t2.p, cbo_t2.q, cbo_t3.x, cbo_t4.b from cbo_t1 join
(select cbo_t2.key as p, cbo_t2.c_int as q, c_float as r from cbo_t2) cbo_t2 on cbo_t1.key=p right outer join
(select cbo_t3.key as x, cbo_t3.c_int as y, c_float as z from cbo_t3) cbo_t3 on cbo_t1.key=x left outer join
(select key as a, c_int as b, c_float as c from cbo_t1) cbo_t4 on cbo_t1.key=a;
