set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

CREATE TABLE src_nontemp AS SELECT * FROM src limit 10;
CREATE TEMPORARY TABLE src_temp AS SELECT * FROM src limit 10;

-- Non temp table join
EXPLAIN
FROM src_nontemp src1 JOIN src_nontemp src2 ON (src1.key = src2.key)
SELECT src1.key, src2.value;

FROM src_nontemp src1 JOIN src_nontemp src2 ON (src1.key = src2.key)
SELECT src1.key, src2.value;

-- Non temp table join with temp table
EXPLAIN
FROM src_nontemp src1 JOIN src_temp src2 ON (src1.key = src2.key)
SELECT src1.key, src2.value;

FROM src_nontemp src1 JOIN src_temp src2 ON (src1.key = src2.key)
SELECT src1.key, src2.value;

-- temp table join with temp table
EXPLAIN
FROM src_temp src1 JOIN src_temp src2 ON (src1.key = src2.key)
SELECT src1.key, src2.value;

FROM src_temp src1 JOIN src_temp src2 ON (src1.key = src2.key)
SELECT src1.key, src2.value;

DROP TABLE src_nontemp;
DROP TABLE src_temp;
