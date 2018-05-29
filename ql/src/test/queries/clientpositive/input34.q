--! qt:dataset:src
CREATE TABLE dest1_n161(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
  USING 'cat'
  AS (tkey, tvalue) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
) tmap
INSERT OVERWRITE TABLE dest1_n161 SELECT tkey, tvalue;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
  USING 'cat'
  AS (tkey, tvalue) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
) tmap
INSERT OVERWRITE TABLE dest1_n161 SELECT tkey, tvalue;

SELECT dest1_n161.* FROM dest1_n161;
