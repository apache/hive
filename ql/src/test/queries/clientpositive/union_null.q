-- SORT_BEFORE_DIFF

-- HIVE-2901
select x from (select * from (select value as x from src order by x limit 5)a union all select * from (select NULL as x from src limit 5)b )a;
set hive.cbo.returnpath.hiveop=true; 
select x from (select * from (select value as x from src order by x limit 5)a union all select * from (select NULL as x from src limit 5)b )a;

set hive.cbo.returnpath.hiveop=false;
-- HIVE-4837
select * from (select * from (select null as N from src1 group by key)a UNION ALL select * from (select null as N from src1 group by key)b ) a;
