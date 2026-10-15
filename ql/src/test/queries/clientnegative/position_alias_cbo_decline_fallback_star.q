-- HIVE-30037: under a fallback strategy other than NEVER, an invalid position recompiles the declined statement
-- without CBO, which rejects positions with SELECT *, as for a statement that CBO plans
set hive.cbo.fallback.strategy=CONSERVATIVE;
create table pacdfs_t (d int, s string);
select * from pacdfs_t tablesample (5 rows) order by 9;
