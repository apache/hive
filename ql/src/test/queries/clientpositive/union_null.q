-- SORT_BEFORE_DIFF

-- HIVE-2901
select x from (select * from (select value as x from src order by x limit 5)a union all select * from (select NULL as x from src limit 5)b )a;

-- HIVE-4837
select * from (select * from (select null as N from src1 group by key)a UNION ALL select * from (select null as N from src1 group by key)b ) a;
