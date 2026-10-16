-- HIVE-30037: under a fallback strategy other than NEVER, an invalid position makes Hive recompile the declined
-- statement without CBO, which rejects ORDER BY positions with SELECT *, as it does for a statement that CBO plans
set hive.cbo.fallback.strategy=CONSERVATIVE;
create table pacdfs_t (d int, s string);
select * from pacdfs_t tablesample (5 rows) order by 9;
