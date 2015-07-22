set hive.explain.user=false;
explain 
SELECT key, value FROM
(
  SELECT key, value FROM src
  UNION ALL
  SELECT key, key as value FROM ( 
    SELECT distinct key FROM (
      SELECT key, value FROM (
        SELECT key, value FROM src
        UNION ALL
        SELECT key, value FROM src
      )t1 
    group by key, value)t2
  )t3
)t4
group by key, value;

SELECT key, value FROM
(
  SELECT key, value FROM src
  UNION ALL
  SELECT key, key as value FROM ( 
    SELECT distinct key FROM (
      SELECT key, value FROM (
        SELECT key, value FROM src
        UNION ALL
        SELECT key, value FROM src
      )t1 
    group by key, value)t2
  )t3
)t4
group by key, value;
