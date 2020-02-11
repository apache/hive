set hive.mapred.mode=nonstrict;
set hive.optimize.skewjoin.compiletime = true;

CREATE TABLE tmpT1_n109(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE tmpT1_n109;

-- testing skew on other data types - int
CREATE TABLE T1_n109(key INT, val STRING) SKEWED BY (key) ON ((2));
INSERT OVERWRITE TABLE T1_n109 SELECT key, val FROM tmpT1_n109;

CREATE TABLE tmpT2_n66(key STRING, val STRING) STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/T2.txt' INTO TABLE tmpT2_n66;

CREATE TABLE T2_n66(key INT, val STRING) SKEWED BY (key) ON ((3));

INSERT OVERWRITE TABLE T2_n66 SELECT key, val FROM tmpT2_n66;

-- The skewed key is a integer column.
-- Otherwise this test is similar to skewjoinopt1.q
-- Both the joined tables are skewed, and the joined column
-- is an integer
-- adding a order by at the end to make the results deterministic

EXPLAIN
SELECT a.*, b.* FROM T1_n109 a JOIN T2_n66 b ON a.key = b.key;

SELECT a.*, b.* FROM T1_n109 a JOIN T2_n66 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;

-- test outer joins also

EXPLAIN
SELECT a.*, b.* FROM T1_n109 a RIGHT OUTER JOIN T2_n66 b ON a.key = b.key;

SELECT a.*, b.* FROM T1_n109 a RIGHT OUTER JOIN T2_n66 b ON a.key = b.key
ORDER BY a.key, b.key, a.val, b.val;

-- an aggregation at the end should not change anything

EXPLAIN
SELECT count(1) FROM T1_n109 a JOIN T2_n66 b ON a.key = b.key;

SELECT count(1) FROM T1_n109 a JOIN T2_n66 b ON a.key = b.key;

EXPLAIN
SELECT count(1) FROM T1_n109 a RIGHT OUTER JOIN T2_n66 b ON a.key = b.key;

SELECT count(1) FROM T1_n109 a RIGHT OUTER JOIN T2_n66 b ON a.key = b.key;
