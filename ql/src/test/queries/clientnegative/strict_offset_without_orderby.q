--! qt:dataset:src
set hive.strict.checks.offset.no.orderby=false;
set hive.mapred.mode=strict;

EXPLAIN
SELECT * from src limit 5, 10;

SELECT * from src limit 5, 10;
