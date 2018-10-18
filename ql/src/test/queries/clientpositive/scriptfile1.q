--! qt:dataset:src

set hive.vectorized.execution.enabled=false;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- SORT_QUERY_RESULTS

-- NO_SESSION_REUSE

CREATE TABLE dest1_n22(key INT, value STRING);

ADD FILE ../../ql/src/test/scripts/testgrep;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value)
         USING 'testgrep' AS (tkey, tvalue)
  CLUSTER BY tkey
) tmap
INSERT OVERWRITE TABLE dest1_n22 SELECT tmap.tkey, tmap.tvalue;

SELECT dest1_n22.* FROM dest1_n22;
