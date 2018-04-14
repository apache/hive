--! qt:dataset:srcpart
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

EXPLAIN EXTENDED
SELECT x.* FROM SRCPART x WHERE key IS NOT NULL AND ds = '2008-04-08';

SELECT x.* FROM SRCPART x WHERE key IS NOT NULL AND ds = '2008-04-08';

