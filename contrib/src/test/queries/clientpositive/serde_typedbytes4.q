add jar ${system:maven.local.repository}/org/apache/hive/hive-contrib/${system:hive.version}/hive-contrib-${system:hive.version}.jar;

drop table dest1;
CREATE TABLE dest1(key STRING, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM (
  FROM src
  SELECT TRANSFORM(cast(src.key as tinyint), src.value) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.TypedBytesSerDe'
  RECORDWRITER 'org.apache.hadoop.hive.contrib.util.typedbytes.TypedBytesRecordWriter'
  USING 'cat'
  AS (tkey, tvalue) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.TypedBytesSerDe'
  RECORDREADER 'org.apache.hadoop.hive.contrib.util.typedbytes.TypedBytesRecordReader'
  WHERE key < 100
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tkey, tvalue ORDER by tkey, tvalue;

FROM (
  FROM src
  SELECT TRANSFORM(cast(src.key as tinyint), src.value) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.TypedBytesSerDe'
  RECORDWRITER 'org.apache.hadoop.hive.contrib.util.typedbytes.TypedBytesRecordWriter'
  USING 'cat'
  AS (tkey, tvalue) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.TypedBytesSerDe'
  RECORDREADER 'org.apache.hadoop.hive.contrib.util.typedbytes.TypedBytesRecordReader'
  WHERE key < 100
) tmap
INSERT OVERWRITE TABLE dest1 SELECT tkey, tvalue ORDER by tkey, tvalue;

SELECT dest1.* FROM dest1;

drop table dest1;
