reset hive.mapred.mode;
set hive.strict.checks.orderby.no.limit=true;

EXPLAIN
SELECT src.key, src.value from src order by src.key;

SELECT src.key, src.value from src order by src.key;

