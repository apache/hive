--! qt:dataset:src
set hive.explain.user=false;
-- SORT_QUERY_RESULTS

explain
SELECT *
FROM (
  SELECT 1 AS id
  FROM (SELECT * FROM src LIMIT 1) s1
  UNION ALL
  SELECT 2 AS id
  FROM (SELECT * FROM src LIMIT 1) s1
  UNION ALL
  SELECT 3 AS id
  FROM (SELECT * FROM src LIMIT 1) s2
  UNION ALL
  SELECT 4 AS id
  FROM (SELECT * FROM src LIMIT 1) s2
  CLUSTER BY id
) a;



CREATE TABLE union_out_n0 (id int);

insert overwrite table union_out_n0 
SELECT *
FROM (
  SELECT 1 AS id
  FROM (SELECT * FROM src LIMIT 1) s1
  UNION ALL
  SELECT 2 AS id
  FROM (SELECT * FROM src LIMIT 1) s1
  UNION ALL
  SELECT 3 AS id
  FROM (SELECT * FROM src LIMIT 1) s2
  UNION ALL
  SELECT 4 AS id
  FROM (SELECT * FROM src LIMIT 1) s2
  CLUSTER BY id
) a;

select * from union_out_n0;
