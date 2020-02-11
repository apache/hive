set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;
set hive.auto.convert.join=true;

CREATE TABLE T1_n25(key STRING, val STRING)
SKEWED BY (key) ON ((2), (8)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n25;

CREATE TABLE T2_n16(key STRING, val STRING)
SKEWED BY (key) ON ((3), (8)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n16;

-- copy from skewjoinopt3
-- test compile time skew join and auto map join
-- a simple query with skew on both the tables. One of the skewed
-- value is common to both the tables. The skewed value should not be
-- repeated in the filter.
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1_n25 a JOIN T2_n16 b ON a.key = b.key;

SELECT a.*, b.* FROM T1_n25 a JOIN T2_n16 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;

-- test outer joins also

EXPLAIN
SELECT a.*, b.* FROM T1_n25 a FULL OUTER JOIN T2_n16 b ON a.key = b.key;

SELECT a.*, b.* FROM T1_n25 a FULL OUTER JOIN T2_n16 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;
