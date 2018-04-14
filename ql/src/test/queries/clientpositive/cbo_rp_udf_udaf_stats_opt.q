--! qt:dataset:cbo_t1
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;
set hive.cbo.returnpath.hiveop=true;
set hive.exec.check.crossproducts=false;
set hive.compute.query.using.stats=true

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- SORT_QUERY_RESULTS

-- 8. Test UDF/UDAF
select count(*), count(c_int), sum(c_int), avg(c_int), max(c_int), min(c_int) from cbo_t1;
select count(*), count(c_int) as a, sum(c_int), avg(c_int), max(c_int), min(c_int), case c_int when 0  then 1 when 1 then 2 else 3 end, sum(case c_int when 0  then 1 when 1 then 2 else 3 end) from cbo_t1 group by c_int order by a;
select * from (select count(*) as a, count(distinct c_int) as b, sum(c_int) as c, avg(c_int) as d, max(c_int) as e, min(c_int) as f from cbo_t1) cbo_t1;
select * from (select count(*) as a, count(distinct c_int) as b, sum(c_int) as c, avg(c_int) as d, max(c_int) as e, min(c_int) as f, case c_int when 0  then 1 when 1 then 2 else 3 end as g, sum(case c_int when 0  then 1 when 1 then 2 else 3 end) as h from cbo_t1 group by c_int) cbo_t1 order by a;
select f,a,e,b from (select count(*) as a, count(c_int) as b, sum(c_int) as c, avg(c_int) as d, max(c_int) as e, min(c_int) as f from cbo_t1) cbo_t1;
select f,a,e,b from (select count(*) as a, count(distinct c_int) as b, sum(distinct c_int) as c, avg(distinct c_int) as d, max(distinct c_int) as e, min(distinct c_int) as f from cbo_t1) cbo_t1;
select key,count(c_int) as a, avg(c_float) from cbo_t1 group by key order by a;
select count(distinct c_int) as a, avg(c_float) from cbo_t1 group by c_float order by a;
select count(distinct c_int) as a, avg(c_float) from cbo_t1 group by c_int order by a;
select count(distinct c_int) as a, avg(c_float) from cbo_t1 group by c_float, c_int order by a;
