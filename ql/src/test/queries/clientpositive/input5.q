--! qt:dataset:src_thrift
CREATE TABLE dest1_n94(key STRING, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM (
  FROM src_thrift
  SELECT TRANSFORM(src_thrift.lint, src_thrift.lintstring)
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1_n94 SELECT tmap.tkey, tmap.tvalue;

FROM (
  FROM src_thrift
  SELECT TRANSFORM(src_thrift.lint, src_thrift.lintstring)
         USING 'cat' AS (tkey, tvalue) 
  CLUSTER BY tkey 
) tmap
INSERT OVERWRITE TABLE dest1_n94 SELECT tmap.tkey, tmap.tvalue;

SELECT dest1_n94.* FROM dest1_n94;
