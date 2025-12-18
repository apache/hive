CREATE TABLE t1 (cat INT, val BIGINT, data STRING);
ALTER TABLE t1 UPDATE STATISTICS SET('numRows'='1000000','rawDataSize'='100000000');
ALTER TABLE t1 UPDATE STATISTICS FOR COLUMN cat SET('numDVs'='100','numNulls'='0');

-- Test 1: IF should result in NDV of 2
EXPLAIN
SELECT x, COUNT(*)
FROM (SELECT IF(cat > 50, 'A', 'B') x FROM t1) sub
GROUP BY x;

-- Test 2: CASE WHEN should result in NDV of 3
EXPLAIN
SELECT x, COUNT(*)
FROM (
  SELECT CASE WHEN cat < 30 THEN 'X' WHEN cat < 60 THEN 'Y' ELSE 'Z' END x
  FROM t1
) sub
GROUP BY x;

-- Test 3: CASE col WHEN val should result in NDV of 4
EXPLAIN
SELECT x, COUNT(*)
FROM (
  SELECT CASE cat WHEN 1 THEN 'A' WHEN 2 THEN 'B' WHEN 3 THEN 'C' ELSE 'D' END x
  FROM t1
) sub
GROUP BY x;

-- Test 4: MapJoin NO longer chosen due to NDV=1 causing tiny size estimate
CREATE TABLE t2 (key STRING, v1 STRING);

ALTER TABLE t1 UPDATE STATISTICS FOR COLUMN val SET('numDVs'='1000000','numNulls'='0');
ALTER TABLE t1 UPDATE STATISTICS FOR COLUMN data SET('numDVs'='5000000','numNulls'='0','avgColLen'='500.0','maxColLen'='600');
ALTER TABLE t2 UPDATE STATISTICS SET('numRows'='1000000','rawDataSize'='100000000');
ALTER TABLE t2 UPDATE STATISTICS FOR COLUMN key SET('numDVs'='1000000','numNulls'='0','avgColLen'='50.0','maxColLen'='100');
ALTER TABLE t2 UPDATE STATISTICS FOR COLUMN v1 SET('numDVs'='1000000','numNulls'='0','avgColLen'='50.0','maxColLen'='100');
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask.size=1000;

EXPLAIN
SELECT a.k, a.total, a.sample, b.v1
FROM (
  SELECT
    k,
    SUM(val) as total,
    MAX(data) as sample
  FROM (
    SELECT
      CASE
        WHEN cat BETWEEN 0 AND 4 THEN 'K00'
        WHEN cat BETWEEN 5 AND 9 THEN 'K01'
        WHEN cat BETWEEN 10 AND 14 THEN 'K02'
        WHEN cat BETWEEN 15 AND 19 THEN 'K03'
        WHEN cat BETWEEN 20 AND 24 THEN 'K04'
        WHEN cat BETWEEN 25 AND 29 THEN 'K05'
        WHEN cat BETWEEN 30 AND 34 THEN 'K06'
        WHEN cat BETWEEN 35 AND 39 THEN 'K07'
        WHEN cat BETWEEN 40 AND 44 THEN 'K08'
        WHEN cat BETWEEN 45 AND 49 THEN 'K09'
        WHEN cat BETWEEN 50 AND 54 THEN 'K10'
        WHEN cat BETWEEN 55 AND 59 THEN 'K11'
        WHEN cat BETWEEN 60 AND 64 THEN 'K12'
        WHEN cat BETWEEN 65 AND 69 THEN 'K13'
        WHEN cat BETWEEN 70 AND 74 THEN 'K14'
        WHEN cat BETWEEN 75 AND 79 THEN 'K15'
        WHEN cat BETWEEN 80 AND 84 THEN 'K16'
        WHEN cat BETWEEN 85 AND 89 THEN 'K17'
        WHEN cat BETWEEN 90 AND 94 THEN 'K18'
        ELSE 'K19'
      END as k,
      val,
      data
    FROM t1
  ) s
  GROUP BY k
) a
JOIN t2 b ON a.k = b.key;
