-- Confirms HIVE-29559 fixes the HIVE-28724 NPE on the CBO fallback recompile path.
set hive.cbo.fallback.strategy=ALWAYS;

explain
WITH cte AS (
    SELECT MAX(s) AS m FROM (SELECT 'a' AS s) t
)
SELECT s FROM (SELECT 'a' AS s) u WHERE s = ALL(SELECT m FROM cte)
UNION ALL
SELECT s FROM (SELECT 'a' AS s) u WHERE s = ALL(SELECT m FROM cte)
UNION ALL
SELECT s FROM (SELECT 'a' AS s) u WHERE s = ALL(SELECT m FROM cte);
