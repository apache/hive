set hive.mapred.mode=nonstrict;
set hive.groupby.skewindata=true;
-- SORT_BEFORE_DIFF
-- This tests that a union all with a map only subquery on one side and a 
-- subquery involving two map reduce jobs on the other runs correctly.

CREATE TABLE test_src_n1 (key STRING, value STRING);

EXPLAIN INSERT OVERWRITE TABLE test_src_n1 
SELECT key, value FROM (
	SELECT key, value FROM src 
	WHERE key = 0
UNION ALL
 	SELECT key, cast(COUNT(*) as string) AS value FROM src
 	GROUP BY key
)a;
 
INSERT OVERWRITE TABLE test_src_n1 
SELECT key, value FROM (
	SELECT key, value FROM src 
	WHERE key = 0
UNION ALL
 	SELECT key, cast(COUNT(*) as string) AS value FROM src
 	GROUP BY key
)a;
 
SELECT COUNT(*) FROM test_src_n1;
 
EXPLAIN INSERT OVERWRITE TABLE test_src_n1 
SELECT key, value FROM (
	SELECT key, cast(COUNT(*) as string) AS value FROM src
 	GROUP BY key
UNION ALL
 	SELECT key, value FROM src 
	WHERE key = 0
)a;
 
INSERT OVERWRITE TABLE test_src_n1 
SELECT key, value FROM (
	SELECT key, cast(COUNT(*) as string) AS value FROM src
 	GROUP BY key
UNION ALL
 	SELECT key, value FROM src 
	WHERE key = 0
)a;
 
SELECT COUNT(*) FROM test_src_n1;
 
