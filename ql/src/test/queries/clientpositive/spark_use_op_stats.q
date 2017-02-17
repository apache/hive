set hive.mapred.mode=nonstrict;
set hive.spark.use.op.stats=false;
set hive.auto.convert.join=false;
set hive.exec.reducers.bytes.per.reducer=500;

EXPLAIN
SELECT src1.key, src2.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
WHERE src1.key = 97;

SELECT src1.key, src2.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
WHERE src1.key = 97;

CREATE TEMPORARY TABLE tmp AS
SELECT * FROM src WHERE key > 50 AND key < 200;

EXPLAIN
WITH a AS (
  SELECT src1.key, src2.value
  FROM tmp src1 JOIN tmp src2 ON (src1.key = src2.key)
  WHERE src1.key > 100
),
b AS (
  SELECT src1.key, src2.value
  FROM src src1 JOIN src src2 ON (src1.key = src2.key)
  WHERE src1.key > 150
)
SELECT sum(hash(a.key, b.value)) FROM a JOIN b ON a.key = b.key;

WITH a AS (
  SELECT src1.key, src2.value
  FROM tmp src1 JOIN tmp src2 ON (src1.key = src2.key)
  WHERE src1.key > 100
),
b AS (
  SELECT src1.key, src2.value
  FROM src src1 JOIN src src2 ON (src1.key = src2.key)
  WHERE src1.key > 150
)
SELECT sum(hash(a.key, b.value)) FROM a JOIN b ON a.key = b.key;
