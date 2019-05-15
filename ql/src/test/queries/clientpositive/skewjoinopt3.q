set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE T1_n12(key STRING, val STRING)
SKEWED BY (key) ON ((2), (8)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n12;

CREATE TABLE T2_n7(key STRING, val STRING)
SKEWED BY (key) ON ((3), (8)) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE T2_n7;

-- a simple query with skew on both the tables. One of the skewed
-- value is common to both the tables. The skewed value should not be
-- repeated in the filter.
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1_n12 a JOIN T2_n7 b ON a.key = b.key;

SELECT a.*, b.* FROM T1_n12 a JOIN T2_n7 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;

-- test outer joins also

EXPLAIN
SELECT a.*, b.* FROM T1_n12 a FULL OUTER JOIN T2_n7 b ON a.key = b.key;

SELECT a.*, b.* FROM T1_n12 a FULL OUTER JOIN T2_n7 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;
