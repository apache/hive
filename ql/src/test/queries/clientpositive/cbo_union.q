--! qt:dataset:cbo_t3
--! qt:dataset:cbo_t2
--! qt:dataset:cbo_t1
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;
set hive.exec.check.crossproducts=false;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- SORT_QUERY_RESULTS

-- 11. Union All
select * from (select * from cbo_t1 order by key, c_boolean, value, dt)a union all select * from (select * from cbo_t2 order by key, c_boolean, value, dt)b;
select key from (select key, c_int from (select * from cbo_t1 union all select * from cbo_t2 where cbo_t2.key >=0)r1 union all select key, c_int from cbo_t3)r2 where key >=0 order by key;
select r2.key from (select key, c_int from (select key, c_int from cbo_t1 union all select key, c_int from cbo_t3 )r1 union all select key, c_int from cbo_t3)r2 join   (select key, c_int from (select * from cbo_t1 union all select * from cbo_t2 where cbo_t2.key >=0)r1 union all select key, c_int from cbo_t3)r3 on r2.key=r3.key where r3.key >=0 order by r2.key;

