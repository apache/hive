-- VARIANT read through LLAP, vectorized and in row mode: an Iceberg Parquet table (LLAP IO caches its metadata), native
-- temporary ORC (the IO elevator) and TEXTFILE (the encode cache) tables, and an Iceberg Avro table, which runs
-- without vectorization or LLAP IO. An Iceberg ORC table cannot hold VARIANT: Iceberg's ORC writer rejects it when it
-- computes the file metrics.
set hive.llap.io.enabled=true;
set hive.fetch.task.conversion=none;
set hive.explain.user=false;

CREATE EXTERNAL TABLE vllap_parquet (id INT, v VARIANT) STORED BY ICEBERG STORED AS PARQUET
TBLPROPERTIES ('format-version'='3');
INSERT INTO vllap_parquet VALUES
(1, parse_json('{"a":1,"b":"x"}')), (2, parse_json('{"a":2,"b":[true,null]}')), (3, parse_json('"é"')),
(4, parse_json('null')), (5, CAST(NULL AS VARIANT)), (6, parse_json('{"a":1,"b":"x"}'));
CREATE TEMPORARY TABLE vllap_orc (id INT, v VARIANT) STORED AS ORC;
INSERT INTO vllap_orc SELECT id, v FROM vllap_parquet;
CREATE TEMPORARY TABLE vllap_text (id INT, v VARIANT) STORED AS TEXTFILE;
INSERT INTO vllap_text SELECT id, v FROM vllap_parquet;
CREATE EXTERNAL TABLE vllap_avro (id INT, v VARIANT) STORED BY ICEBERG STORED AS AVRO
TBLPROPERTIES ('format-version'='3');
INSERT INTO vllap_avro SELECT id, v FROM vllap_parquet;

set hive.vectorized.execution.enabled=true;
EXPLAIN WITH u AS (SELECT 'orc' f, id, v FROM vllap_orc UNION ALL SELECT 'parquet', id, v FROM vllap_parquet
  UNION ALL SELECT 'text', id, v FROM vllap_text)
SELECT f, id, v, to_json(v) FROM u WHERE variant_get(v, '$.a', 'int') = 1;
WITH u AS (SELECT 'orc' f, id, v FROM vllap_orc UNION ALL SELECT 'parquet', id, v FROM vllap_parquet
  UNION ALL SELECT 'text', id, v FROM vllap_text)
SELECT f, id, v, to_json(v) FROM u WHERE variant_get(v, '$.a', 'int') = 1 ORDER BY f, id;
WITH u AS (SELECT 'orc' f, id, v FROM vllap_orc UNION ALL SELECT 'parquet', id, v FROM vllap_parquet
  UNION ALL SELECT 'text', id, v FROM vllap_text)
SELECT f, v, count(*) FROM u GROUP BY f, v ORDER BY f, v;

set hive.vectorized.execution.enabled=false;
EXPLAIN WITH u AS (SELECT 'orc' f, id, v FROM vllap_orc UNION ALL SELECT 'parquet', id, v FROM vllap_parquet
  UNION ALL SELECT 'text', id, v FROM vllap_text)
SELECT f, id, v, to_json(v) FROM u WHERE variant_get(v, '$.a', 'int') = 1;
WITH u AS (SELECT 'orc' f, id, v FROM vllap_orc UNION ALL SELECT 'parquet', id, v FROM vllap_parquet
  UNION ALL SELECT 'text', id, v FROM vllap_text)
SELECT f, id, v, to_json(v) FROM u WHERE variant_get(v, '$.a', 'int') = 1 ORDER BY f, id;
WITH u AS (SELECT 'orc' f, id, v FROM vllap_orc UNION ALL SELECT 'parquet', id, v FROM vllap_parquet
  UNION ALL SELECT 'text', id, v FROM vllap_text)
SELECT f, v, count(*) FROM u GROUP BY f, v ORDER BY f, v;

SELECT id, v, to_json(v) FROM vllap_avro WHERE variant_get(v, '$.a', 'int') = 1 ORDER BY id;
SELECT v, count(*) FROM vllap_avro GROUP BY v ORDER BY v;
