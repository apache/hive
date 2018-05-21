--! qt:dataset:src_thrift
CREATE TABLE dest1_n81(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM (
  FROM src_thrift
  SELECT TRANSFORM(src_thrift.aint + src_thrift.lint[0], src_thrift.lintstring[0])
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1_n81 SELECT tmap.tkey, tmap.tvalue;

FROM (
  FROM src_thrift
  SELECT TRANSFORM(src_thrift.aint + src_thrift.lint[0], src_thrift.lintstring[0])
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1_n81 SELECT tmap.tkey, tmap.tvalue;

-- SORT_QUERY_RESULTS

SELECT dest1_n81.* FROM dest1_n81;
