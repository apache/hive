--! qt:dataset:src
reset hive.mapred.mode;
set hive.strict.checks.offset.no.orderby=true;

EXPLAIN
SELECT * from src limit 5, 10;

SELECT * from src limit 5, 10;
