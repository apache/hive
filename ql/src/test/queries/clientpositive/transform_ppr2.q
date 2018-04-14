--! qt:dataset:srcpart
--! qt:dataset:src
set hive.optimize.ppd=true;
set hive.entity.capture.transform=true;

-- SORT_QUERY_RESULTS

EXPLAIN EXTENDED
FROM (
  FROM srcpart src
  SELECT TRANSFORM(src.ds, src.key, src.value)
         USING 'cat' AS (ds, tkey, tvalue)
  WHERE src.ds = '2008-04-08' 
  CLUSTER BY tkey 
) tmap
SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

FROM (
  FROM srcpart src
  SELECT TRANSFORM(src.ds, src.key, src.value)
         USING 'cat' AS (ds, tkey, tvalue) 
  WHERE src.ds = '2008-04-08' 
  CLUSTER BY tkey 
) tmap
SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100;

