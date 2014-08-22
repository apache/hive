-- SORT_BEFORE_DIFF

-- HIVE-2901
select x from (select value as x from src order by x limit 5 union all select NULL as x from src limit 5) a;

-- HIVE-4837
select * from (select null as N from src1 group by key UNION ALL select null as N from src1 group by key ) a;
