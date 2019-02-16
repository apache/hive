--! qt:dataset:src1
--! qt:dataset:src

-- SORT_QUERY_RESULTS

-- HIVE-2901
select x from (select * from (select value as x from src order by x limit 5)a union all select * from (select cast(NULL as string) as x from src limit 5)b )a;
set hive.cbo.returnpath.hiveop=true;
select x from (select * from (select value as x from src order by x limit 5)a union all select * from (select cast(NULL as string) as x from src limit 5)b )a;

set hive.cbo.returnpath.hiveop=false;
-- HIVE-4837
select * from (select * from (select cast(null as string) as N from src1 group by key)a UNION ALL select * from (select cast(null as string) as N from src1 group by key)b ) a;

-- HIVE-16050
select null as c1 UNION ALL select 1 as c1;
