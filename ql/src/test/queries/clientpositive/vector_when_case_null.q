set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

create table count_case_groupby (key string, bool boolean) STORED AS orc;
insert into table count_case_groupby values ('key1', true),('key2', false),('key3', NULL),('key4', false),('key5',NULL);

explain
SELECT key, COUNT(CASE WHEN bool THEN 1 WHEN NOT bool THEN 0 ELSE NULL END) AS cnt_bool0_ok FROM count_case_groupby GROUP BY key;

SELECT key, COUNT(CASE WHEN bool THEN 1 WHEN NOT bool THEN 0 ELSE NULL END) AS cnt_bool0_ok FROM count_case_groupby GROUP BY key;